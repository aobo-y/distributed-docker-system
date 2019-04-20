import re
import docker
import socket
import psutil
import random
import xmlrpc.client
import xmlrpc.server
from threading import Thread
from docker.errors import APIError, ImageNotFound
from arg_parser import get_parser

# global variable
job_executor = None
CONTAINER_MAX_RESTART = 5

class JobExecutor:

    def __init__(self, cpu, memory):
        self.status = "idle"
        self.cpu = cpu
        self.memory = memory # gigabyte
        self.client = docker.from_env()
        self.jobs = {}

    # submit job
    def submit(self, job_dict):
        if job_dict['job_id'] in self.jobs:
            # assume master submit same job twice
            return True
        # check if agent meet job requirement
        if job_dict['resource_requirement']['cpu'] > self.cpu or job_dict['resource_requirement']['memory'] > self.memory:
            # agent not qualified for job
            return False
        # setup and run container
        cpu_limit = min(self.cpu, job_dict['resource_limit']['cpu'])
        # set cpu limit
        cpus = [str(i) for i in range(self.cpu)]
        random.shuffle(cpus)
        usable_cpu_str = ','.join(cpus[:cpu_limit])
        # set mem limit
        mem_limit = min(self.memory, job_dict['resource_limit']['memory'])
        mem_limit_str = str(mem_limit)+'g'
        try:
            if job_dict['restart']:
                restart_policy_dict = {"Name": "on-failure", "MaximumRetryCount": CONTAINER_MAX_RESTART}
                job_container = self.client.containers.run(job_dict['image_url'], cpuset_cpus=usable_cpu_str, \
                mem_limit=mem_limit_str, restart_policy=restart_policy_dict, detach=True)
            else:
                job_container = self.client.containers.run(job_dict['image_url'], cpuset_cpus=usable_cpu_str, \
                mem_limit=mem_limit_str, detach=True)
            self.jobs[job_dict['job_id']] = job_container
        except ImageNotFound:
            return False
        except APIError as err:
            print(err)
            return False
        return True

    def check_job(self, job_id):
        if job_id not in self.jobs:
            return "job not exist"
        job_container = self.jobs[job_id]
        return job_container.status

    def heartbeat(self):
        cpu_percentage = psutil.cpu_percent(interval=True)
        memory_percentage = psutil.virtual_memory()[2]
        job_status_list = []
        for job_id, job_container in self.jobs:
            job_status_list.append((job_id, job_container.status))
        pulse_data = {}
        pulse_data['cpu_percentage'] = cpu_percentage
        pulse_data['memory_percentage'] = memory_percentage
        pulse_data['job_status_list'] = job_status_list
        return pulse_data

    def get_output(self, job_id):
        if job_id not in self.jobs:
            return "job not exist"
        job_container = self.jobs[job_id]
        try:
            job_logs = job_container.logs()
            return job_logs
        except APIError:
            return ''
    
    def kill_job(self, job_id):
        if job_id in self.jobs:
            if self.jobs[job_id].status != "exited":
                self.jobs[job_id].kill()

"""
RPC Methods
"""
def rpc_heartbeat():
    # todo
    return job_executor.heartbeat()

def rpc_submit_job(job_dict):
    # todo
    return job_executor.submit(job_dict)

def rpc_stream_output(job_id):
    # todo 
    return job_executor.get_output(job_id)

def rpc_kill_job(job_id):
    job_executor.kill_job(job_id)

def rpc_test(job_id):
    # used for server debugging
    job_executor.check_job(job_id)

"""
Agent Methods
"""
# find self ip and free port
def get_addr_port(agent_status="free"):
    if agent_status == "free":
        # not inside nat or firewall
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        addr, port = s.getsockname()
        s.close()
        return addr, port
    elif agent_status == "loopback":
        # loopback for testing
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind(("", 0))
        addr, port = s.getsockname()
        s.close()
        return addr, port
    else:
        return None, None

def valid_url(input_url):
    url_regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(url_regex, input_url) is not None

# start agent
if __name__ == '__main__':
    # parse args
    parser = get_parser()
    args = parser.parse_args()
    # set & validate master url
    master_url = args.master_url
    if not valid_url(master_url):
        print("invalid master url")
        quit()
    # set up rpc
    cpu, memory = psutil.cpu_count(logical=False), psutil.virtual_memory().total / (1024**3)
    # psutil bug fix: first call to cpu_percent will return 0
    psutil.cpu_percent(interval=None)
    job_executor = JobExecutor(cpu, memory)
    addr, port = get_addr_port("loopback")
    rpc_server = xmlrpc.server.SimpleXMLRPCServer((addr, port), allow_none=True)
    print("agent rpc server listening on port", port)
    # register rpc methods
    rpc_server.register_function(rpc_heartbeat, "heartbeat")
    rpc_server.register_function(rpc_submit_job, "submit_job")
    rpc_server.register_function(rpc_stream_output, "stream_output")
    rpc_server.register_function(rpc_kill_job, "kill_job")
    # this one is for testing & debugging
    rpc_server.register_function(rpc_test, "test")
    rpc_server_thread = Thread(target=lambda server : server.serve_forever(), args=(rpc_server,))
    # run rpc server
    rpc_server_thread.setDaemon(True)
    rpc_server_thread.start()
    # register node to master
    with xmlrpc.client.ServerProxy(master_url) as master:
        try:
            agent_dict = {}
            agent_dict["agent_url"] = "http://"+addr+":"+str(port)
            agent_dict["agent_cpu"] = cpu
            agent_dict["memory"] = memory # gigabytes
            master.register_agent(agent_dict)
        except xmlrpc.client.Error as err:
            # todo: handle failure
            print("Error ", err)
            quit()
    while True:
        # do nothing
        pass
    