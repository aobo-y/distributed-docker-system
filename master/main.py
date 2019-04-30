import re
import time
import uuid
import http
import random
import xmlrpc.client
import xmlrpc.server
from threading import Thread, Lock

class ImageNotFoundError(Exception):
    def __init__(self):
        pass

# Master Configurations
MASTER_PORT = 8888
MASTER_IP = 'localhost'
HEARTBEAT_RATE = 10 # seconds
CPR_PERIODS = [10, 30, 60] # reconnection time periods

# Global Variables
# to maintain consistency, items shall not be deleted from agents and jobs
agents = {} # agent_id -> {'proxy': xmlrpc.client.ServerProxy, 'cpu':int, 'cpu_usage': float, 'memory':float, 'memory_usage':float}
agents_lock = Lock()
jobs = {} # job_id -> {'status': str, 'agent_id': str, 'restart_count':int}
jobs_lock = Lock()

# Internel Methods
def get_id(id_type):
    assert id_type in ['job', 'agent']
    if id_type == 'job':
        id = str(uuid.uuid4())
        while id in jobs:
            id = str(uuid.uuid4())
        return id
    else:
        id = str(uuid.uuid4())
        while id in agents:
            id = str(uuid.uuid4())
        return id

def validate_url(input_url):
    url_regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(url_regex, input_url) is not None

def validate_job(job_dict):
    # client side has checked each field
    if job_dict is not None:
        return True
    else:
        return False

def validate_agent(agent_dict):
    if agent_dict is not None and 'cpu' in agent_dict and 'memory' in agent_dict and 'url' in agent_dict and validate_url(agent_dict['url']):
        return True
    else:
        return False    

def validate_proxy(agent_proxy):
    agent_required_methods = [
        'heartbeat',
        'submit_job',
        'stream_output',
        'kill_job'
    ]
    try:
        agent_methods = agent_proxy.system.listMethods()
        for method in agent_required_methods:
            if method not in agent_methods:
                return False
        return True
    except xmlrpc.client.ProtocolError as err:
        print("xmlrpc.client.ProtocalError: %s" % err.errmsg)
        return False
    except xmlrpc.client.Fault as err:
        print("xmlrpc.client.Fault: %s" % err.faultString)
        return False
    except ConnectionRefusedError as err:
        print("connection error:", str(err))
        return False


# core feature: resource matching
def match_job_to_agent(job_dict):
    # find qualified agent with least workload to do the job
    for agent_id in list(agents):
        if agents[agent_id]['status'] in ['icu', 'dead']:
            continue
        candidates = []
        if agents[agent_id]['cpu'] >= job_dict['resource_requirement']['cpu'] and agents[agent_id]['memory'] >= job_dict['resource_requirement']['memory']:
            candidates.append(agent_id)
        candidates.sort(key=lambda agent_id : agents[agent_id]['cpu_usage'])
        candidates.sort(key=lambda agent_id : agents[agent_id]['memory_usage'])
        for agent_id in candidates:
            try:
                if agents[agent_id]['proxy'].submit_job(job_dict):
                    return agent_id
            except xmlrpc.client.Fault as err:
                if err.faultCode == 1:
                    raise ImageNotFoundError()
            except xmlrpc.client.ProtocolError as ignored:
                continue
            except ConnectionRefusedError as ignored:
                continue
    # no qualified agent
    return None


# RPC Methods
def rpc_submit_job(job_dict):
    if not validate_job(job_dict):
        raise xmlrpc.client.Fault(1, 'invalid job dict')
    job_id = get_id('job')
    job_dict['job_id'] = job_id
    try:
        agent_id = match_job_to_agent(job_dict)
    except ImageNotFoundError:
        raise xmlrpc.client.Fault(1, 'invalid job dict: docker image not exist')
    new_job = {}
    new_job['job_dict'] = job_dict
    new_job['agent_id'] = agent_id
    if agent_id is None:
        new_job['status'] = 'pending'
    else:
        new_job['status'] = 'deploying'
    new_job['restart_count'] = 0
    with jobs_lock:
        jobs[job_id] = new_job
    return job_id


def rpc_register_agent(agent_dict):
    if not validate_agent(agent_dict):
        raise xmlrpc.client.Fault(1, 'invalid agent dict')
    agent_id = get_id('agent')
    new_agent = {}
    new_agent['status'] = 'alive' # agent status in ['alive', 'icu', 'dead']
    new_agent['cpu'] = agent_dict['cpu']
    new_agent['memory'] = agent_dict['memory']
    agent_proxy = xmlrpc.client.ServerProxy(agent_dict['url']) 
    if not validate_proxy(agent_proxy):
        raise xmlrpc.client.Fault(2, 'invalid agent rpc server')
    else:    
        new_agent['proxy'] = agent_proxy
    new_agent['cpu_usage'] = 0.01 # set to nonzero small value for resource matching algorithm
    new_agent['memory_usage'] = 0.01
    with agents_lock:
        agents[agent_id] = new_agent
    print('agent added')
    return True


def rpc_get_status(job_id):
    if job_id not in jobs:
        raise xmlrpc.client.Fault(1, 'job id not exist')
    else:
        return jobs[job_id]['status']


def rpc_kill_job(job_id):
    if job_id not in jobs:
        raise xmlrpc.client.Fault(1, 'job id not exist')
    if jobs[job_id]['status'] in ['end', 'fail']:
        return True
    agent_id = jobs[job_id]['agent_id']
    try:
        return agents[agent_id]['proxy'].kill_job(job_id)
    except xmlrpc.client.ProtocolError as err:
        raise xmlrpc.client.Fault(2, str(err))
    except xmlrpc.client.Fault as err:
        raise xmlrpc.client.Fault(3, str(err))
    except ConnectionRefusedError as err:
        raise xmlrpc.client.Fault(4, str(err))
        

def rpc_output_request(job_id):
    if job_id not in jobs:
        raise xmlrpc.client.Fault(1, 'job id not exist')
    agent_id = jobs[job_id]['agent_id']
    try:
        job_logs = agents[agent_id]['proxy'].stream_output(job_id)
        # type(job_logs) == <class 'xmlrpc.client.Binary'>
        return job_logs
    except xmlrpc.client.Fault as err:
        if err.faultCode == 1:
            raise xmlrpc.client.Fault(1, 'job id not exist')
        else:
            raise xmlrpc.client.Fault(2, err.faultString)
    except xmlrpc.client.ProtocolError as err:
        raise xmlrpc.client.Fault(3, str(err))
    except ConnectionRefusedError as err:
        raise xmlrpc.client.Fault(4, str(err))
    

def rpc_list_jobs():
    results = []
    for job_id in list(jobs):
        job_attrs = {}
        job_attrs['job_id'] = job_id
        job_attrs['job_status'] = jobs[job_id]['status']
        job_attrs['job_restart_count'] = jobs[job_id]['restart_count']
        results.append(job_attrs)
    return results


def rpc_is_even(num):
    return num % 2 == 0


# Heartbeat Methods
def redeploy_job(job_id):
    job_dict = jobs[job_id]['job_dict']
    new_agent_id = None
    try:
        new_agent_id = match_job_to_agent(job_dict)
        with jobs_lock:
            if new_agent_id is not None:
                jobs[job_id]['agent_id'] = new_agent_id
                jobs[job_id]['status'] = 'deploying'
                jobs[job_id]['restart_count'] = 0
            else:
                jobs[job_id]['status'] = 'pending'
    except ImageNotFoundError:
        with jobs_lock:
            jobs[job_id]['status'] = 'fail'


def destroy_agent(agent_id):
    with agents_lock:
        agents[agent_id]['status'] = 'dead'
    for job_id in list(jobs):
        if jobs[job_id]['agent_id'] == agent_id:
            redeploy_job(job_id)
            

def cpr_agent(agent_id):
    for period in CPR_PERIODS:
        time.sleep(period)
        agent_proxy = agents[agent_id]['proxy']
        try:
            agent_pulse = agent_proxy.heartbeat()
            # agent revived
            with agents_lock:
                agents[agent_id]['cpu_usage'] = agent_pulse['cpu_usage']
                agents[agent_id]['memory_usage'] = agent_pulse['memory_usage']
                agents[agent_id]['status'] = 'alive'
            with jobs_lock:
                for job_attrs in agent_pulse['job_attrs_list']:
                    jobs[job_attrs['job_id']]['status'] = job_attrs['status']
                    jobs[job_attrs['job_id']]['restart_count'] = job_attrs['restart_count']
        except xmlrpc.client.Fault:
            pass
        except xmlrpc.client.ProtocolError:
            pass
        except ConnectionRefusedError:
            pass
    destroy_agent(agent_id)


def check_agent_heartbeat(agent_id):
    if agents[agent_id]['status'] in ['icu', 'dead']:
        return
    agent_proxy = agents[agent_id]['proxy']
    try:
        agent_pulse = agent_proxy.heartbeat()
        with agents_lock:
            agents[agent_id]['cpu_usage'] = agent_pulse['cpu_usage']
            agents[agent_id]['memory_usage'] = agent_pulse['memory_usage']
        with jobs_lock:
            for job_attrs in agent_pulse['job_attrs_list']:
                jobs[job_attrs['job_id']]['status'] = job_attrs['status']
                jobs[job_attrs['job_id']]['restart_count'] = job_attrs['restart_count']
    except xmlrpc.client.Fault:
        with agents_lock:
            agents[agent_id]['status'] = 'icu'
        cpr_thread = Thread(target=cpr_agent, args=(agent_id,))
        cpr_thread.start()
    except xmlrpc.client.ProtocolError:
        with agents_lock:
            agents[agent_id]['status'] = 'icu'
        cpr_thread = Thread(target=cpr_agent, args=(agent_id,))
        cpr_thread.start()
    except ConnectionRefusedError:
        with agents_lock:
            agents[agent_id]['status'] = 'icu'
        cpr_thread = Thread(target=cpr_agent, args=(agent_id,))
        cpr_thread.start()
    except http.client.CannotSendRequest:
        with agents_lock:
            agents[agent_id]['status'] = 'icu'
        cpr_thread = Thread(target=cpr_agent, args=(agent_id,))
        cpr_thread.start()
    

def heartbeat(heartbeat_rate):
    while True:
        time.sleep(heartbeat_rate)
        for agent_id in list(agents):
            check_agent_heartbeat(agent_id)
        for job_id in list(jobs):
            if jobs[job_id]['status'] == 'pending':
                redeploy_job(job_id)


if __name__ == '__main__':
    heartbeat_thread = Thread(target=heartbeat, args=(HEARTBEAT_RATE,))
    heartbeat_thread.setDaemon(True)
    heartbeat_thread.start()
    # rpc server
    rpc_server = xmlrpc.server.SimpleXMLRPCServer((MASTER_IP, MASTER_PORT), allow_none=True)
    print("master rpc server listening on port", MASTER_PORT)
    rpc_server.register_function(rpc_get_status, 'get_status')
    rpc_server.register_function(rpc_kill_job, 'kill_job')
    rpc_server.register_function(rpc_list_jobs, 'list_jobs')
    rpc_server.register_function(rpc_output_request, 'output_request')
    rpc_server.register_function(rpc_register_agent, 'register_agent')
    rpc_server.register_function(rpc_submit_job, 'submit_job')
    rpc_server.register_function(rpc_is_even, 'is_even')
    rpc_server.serve_forever()