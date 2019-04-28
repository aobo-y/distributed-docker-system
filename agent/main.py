import re
import time
import docker
import socket
import psutil
import random
import xmlrpc.client
import xmlrpc.server
from threading import Thread
from docker.errors import APIError, ImageNotFound
from arg_parser import get_parser

# Agent Configuration
MAX_RETRY = 5
AGENT_IP = 'localhost'
AGENT_PORT = 8001

# Global Variables
docker_client = docker.from_env()
agent_cpu = psutil.cpu_count(logical=False)
agent_memory = int(psutil.virtual_memory().total / (1024**3))
agent_jobs = {}

# psutil bug fix: first call to cpu_percent will return 0
psutil.cpu_percent(interval=None)

# Internal Methods
def check_job(job_id):
    job_container = agent_jobs[job_id]
    job_container.reload()
    container_status = job_container.status
    container_attrs = job_container.attrs
    job_restart_count = container_attrs['RestartCount']
    # map docker container status to job status in our definition
    # container status: created, restarting, running, removing, paused, exited, or dead
    # job status: pending, deploying, running, end, fail
    job_status = None
    if container_status in ['paused']:
        job_status = 'pending'
    elif container_status in ['created', 'restarting']:
        job_status = 'deploying'
    elif container_status in ['running', 'removing']:
        job_status = 'running'
    elif container_status in ['exited']:
        if container_attrs['State']['ExitCode'] == 0:
            job_status = 'end'
        else:
            job_status = 'fail'
    else:
        job_status = 'fail'
    return job_status, job_restart_count


"""
RPC Methods
"""
def rpc_heartbeat():
    cpu_percentage = psutil.cpu_percent(interval=True)
    memory_percentage = psutil.virtual_memory()[2]
    job_attrs_list = []
    for job_id in agent_jobs:
        job_attrs = {}
        job_attrs['job_id'] = job_id
        job_attrs['status'], job_attrs['restart_count'] = check_job(job_id)
        job_attrs_list.append(job_attrs)
    pulse_data = {}
    pulse_data['cpu_usage'] = cpu_percentage
    pulse_data['memory_usage'] = memory_percentage
    pulse_data['job_attrs_list'] = job_attrs_list
    return pulse_data


def rpc_submit_job(job_dict):
    # setup and run container
    if job_dict['job_id'] in agent_jobs:
        return True
    cpu_limit = min(agent_cpu, job_dict['resource_limit']['cpu'])
    cpus = [str(i) for i in range(agent_cpu)]
    random.shuffle(cpus)
    usable_cpu_str = ','.join(cpus[:cpu_limit])
    mem_limit = min(agent_memory, job_dict['resource_limit']['memory'])
    mem_limit_str = str(mem_limit)+'g'
    try:
        if job_dict['restart']:
            # check restart times
            assert job_dict['restart_times'] > 0 and type(job_dict['restart_times']) == type(1)
            restart_policy_dict = {"Name": "on-failure", "MaximumRetryCount": min(MAX_RETRY, job_dict['restart_times'])}
            job_container = docker_client.containers.run(job_dict['img_url'], cpuset_cpus=usable_cpu_str, \
            mem_limit=mem_limit_str, restart_policy=restart_policy_dict, detach=True)
        else:
            job_container = docker_client.containers.run(job_dict['img_url'], cpuset_cpus=usable_cpu_str, \
            mem_limit=mem_limit_str, detach=True)
        agent_jobs[job_dict['job_id']] = job_container
    except ImageNotFound as err:
        raise xmlrpc.client.Fault(1, 'docker image not exist')
    except APIError as err:
        print(err)
        raise xmlrpc.client.Fault(2, 'docker server error')
    return True


def rpc_stream_output(job_id):
    if job_id not in agent_jobs:
        raise xmlrpc.client.Fault(1, 'job not exist')
    job_container = agent_jobs[job_id]
    job_container.reload()
    try:
        job_logs = job_container.logs()
        # type(job_logs) == <class 'bytes'>
        print(job_logs)
        return job_logs
    except APIError as err:
        raise xmlrpc.client.Fault(2, str(err))


def rpc_kill_job(job_id):
    if job_id not in agent_jobs:
        raise xmlrpc.client.Fault(1, 'job not exist')
    if agent_jobs[job_id].status != "exited":
        try:
            agent_jobs[job_id].kill()
            return True
        except docker.errors.APIError as err:
            print(err)
            raise xmlrpc.client.Fault(2, str(err))
    return True

"""
Agent Methods
"""
def valid_url(input_url):
    url_regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(url_regex, input_url) is not None


def start_agent_rpc_server():
    rpc_server = xmlrpc.server.SimpleXMLRPCServer((AGENT_IP, AGENT_PORT), allow_none=True)
    print("agent rpc server listening on port", AGENT_PORT)
    # register rpc methods
    rpc_server.register_function(rpc_heartbeat, "heartbeat")
    rpc_server.register_function(rpc_submit_job, "submit_job")
    rpc_server.register_function(rpc_stream_output, "stream_output")
    rpc_server.register_function(rpc_kill_job, "kill_job")
    rpc_server.register_introspection_functions()
    rpc_server_thread = Thread(target=lambda server : server.serve_forever(), args=(rpc_server,))
    rpc_server_thread.setDaemon(True)
    rpc_server_thread.start()
    return rpc_server_thread


if __name__ == '__main__':
    parser = get_parser()
    args = parser.parse_args()
    master_url = args.master_url
    if not valid_url(master_url):
        print("invalid master url")
        quit()
    rpc_server_thread = start_agent_rpc_server()
    # register node to master
    with xmlrpc.client.ServerProxy(master_url) as master:
        try:
            agent_dict = {}
            agent_dict["url"] = "http://"+AGENT_IP+":"+str(AGENT_PORT)
            agent_dict["cpu"] = agent_cpu
            agent_dict["memory"] = agent_memory # gigabytes
            master.register_agent(agent_dict)
        except xmlrpc.client.ProtocolError as err:
            print("xmlrpc.client.ProtocalError: %s" % err.errmsg)
            quit()
        except xmlrpc.client.Fault as err:
            print("xmlrpc.client.Fault: %s" % err.faultString)
            quit()
        except ConnectionRefusedError as err:
            print("ConnectionRefusedError: connection refused...")
            quit()
    # wait 
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            quit()