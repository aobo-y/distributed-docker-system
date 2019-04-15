import re
import docker
import socket
import psutil
import xmlrpc.client
import xmlrpc.server
from threading import Thread
from arg_parser import get_parser


class JobExecutor:

    def __init__(self, cpu, memory):
        self.status = "idle"
        self.cpu = cpu
        self.memory = memory
        self.output = None

    # submit job
    def submit(self, job_dict):
        self.job_dict = job_dict

    def check_job(self):
        # todo
        pass

    def start_job(self):
        # todo
        pass

    def heartbeat(self):
        # todo
        pass

    def get_output(self):
        # todo
        pass

# global variable
job_executor = None

"""
RPC Methods
"""
def rpc_heartbeat():
    # todo
    return job_executor.heartbeat()

def rpc_submit_job(job_dict):
    # todo
    job_executor.submit(job_dict)

"""
Agent Methods
"""
# find self ip and free port, assuming node not inside firewall/nat
def get_addr_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    addr, port = s.getsockname()
    s.close()
    return addr, port

def valid_url(input_url):
    url_regex = re.compile(
        r'^(?:http|ftp)s?://' # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
        r'localhost|' #localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
        r'(?::\d+)?' # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE)
    return re.match(url_regex, input_url) is not None

def run_rpc_server(rpc_server):
    print("rpc server listening")
    rpc_server.serve_forever()

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
    cpu, memory = psutil.cpu_count(), psutil.virtual_memory().total / (1024**3)
    job_executor = JobExecutor(cpu, memory)
    addr, port = get_addr_port()
    rpc_server = xmlrpc.server.SimpleXMLRPCServer((addr, port))
    # register rpc methods
    rpc_server.register_function(rpc_heartbeat, "heartbeat")
    rpc_server.register_function(rpc_submit_job, "submit_job")
    rpc_server_thread = Thread(target=run_rpc_server, args=(rpc_server,))
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
    