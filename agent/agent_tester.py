import xmlrpc.client
import xmlrpc.server
from threading import Thread

#test register agent
def register_agent(agent_dict):
    print(agent_dict)

if __name__ == "__main__":
    # setup stub master rpc server
    addr, port = "localhost", 8001
    server = xmlrpc.server.SimpleXMLRPCServer((addr, port), allow_none=True)
    server.register_function(register_agent, "register_agent")
    server_thread = Thread(target=lambda server : server.serve_forever(), args=(server, ))
    server_thread.setDaemon(True)
    server_thread.start()
    print("master listening on port", port)
    # submit simple_job_dict to agent
    agent_url = "http://localhost:8002"
    with xmlrpc.client.ServerProxy(agent_url) as agent:
        try:
            # test simple docker job
            simple_job_dict = {}
            simple_job_dict['job_id'] = 'id0'
            simple_job_dict['image_url'] = 'qizixi/barcode-generator:v0'
            simple_job_dict['resource_requirement'] = {'cpu':1, 'memory':1}
            simple_job_dict['resource_limit'] = {'cpu':1, 'memory':1}
            simple_job_dict['restart'] = True
            submission_result = agent.submit_job(simple_job_dict)
            print("submission_result:", submission_result)
            agent_output = agent.stream_output(simple_job_dict['job_id'])
            print("agent_output:",agent_output)
            agent.test(simple_job_dict['job_id'])
        except xmlrpc.client.Error as err:
            # todo: handle failure
            print("Error ", err)
            quit()