import re
import uuid
import random
import xmlrpc.client

# Master Configurations
MASTER_PORT = 8888
MASTER_IP = 'localhost'
HEARTBEAT_RATE = 10

# Global Variables
agents = {}
jobs = {}

# Internel Methods
def get_id(id_type):
    if id_type == 'job':
        id = uuid.uuid4()
        while id in jobs:
            id = uuid.uuid4()
        return id
    elif id_type == 'agent':
        id = uuid.uuid4()
        while id in agents:
            id = uuid.uuid4()
        return id
    else:
        raise Exception('invalid id type')

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
        agent_methods = agent_proxy.listMethods()
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
        print("ConnectionRefusedError: connection refused...")
        return False


def match_job_to_node(job_dict):
    # find qualified agent with least workload to do the job
    for agent_id in agents:
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
                    raise Exception('docker image not exist')
    # no qualified agent
    return None
    

# RPC Methods
def rpc_submit_job(job_dict):
    if not validate_job(job_dict):
        raise xmlrpc.client.Fault(1, 'invalid job dict')
    job_id = get_id('job')
    job_dict['job_id'] = job_id
    try:
        agent_id = match_job_to_node(job_dict)
    except Exception as e:
        raise xmlrpc.client.Fault(1, 'invalid job dict: docker image not exist')
    if agent_id is None:
        raise xmlrpc.client.Fault(2, 'no qualified agent')
    jobs[job_id] = {}
    jobs[job_id]['job_dict'] = job_dict
    jobs[job_id]['agent_id'] = agent_id
    jobs[job_id]['status'] = 'pending'
    return job_id


def rpc_register_agent(agent_dict):
    if not validate_agent(agent_dict):
        raise xmlrpc.client.Fault(1, 'invalid agent dict')
    agent_id = get_id('agent')
    agents[agent_id] = {}
    agents[agent_id]['cpu'] = agent_dict['cpu']
    agents[agent_id]['memory'] = agent_dict['memory']
    agent_proxy = xmlrpc.client.ServerProxy(agent_dict['url']) 
    if not validate_proxy(agent_proxy):
        raise xmlrpc.client.Fault(2, 'invalid agent rpc server')
    else:    
        agents[agent_id]['proxy'] = agent_proxy
    agents[agent_id]['cpu_usage'] = 0.01
    agents[agent_id]['memory_usage'] = 0.01
    return True


def rpc_get_status(job_id):
    if job_id not in jobs:
        raise xmlrpc.client.Fault(1, 'job id not exist')
    else:
        return jobs[job_id]['status']


def rpc_kill_job(job_id):
    if job_id not in jobs:
        raise xmlrpc.client.Fault(1, 'job id not exist')
    try:
        kill_result = jobs[job_id]['proxy'].kill_job(job_id)
        if kill_result == 'success':
            return True
        else:
            raise xmlrpc.client.Fault(2, kill_result)
    except xmlrpc.client.ProtocolError as err:
        raise xmlrpc.client.Fault(3, 'agent connection error'+err.errmsg)
    except xmlrpc.client.Fault as err:
        print("xmlrpc.client.Fault: %s" % err.faultString)
        return False


def rpc_output_request(job_id):
    if job_id not in jobs:
        raise xmlrpc.client.Fault(1, 'job id not exist')
    
    