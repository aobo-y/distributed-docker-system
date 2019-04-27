import xmlrpc.client
import xmlrpc.server


if __name__ == "__main__":
    # submit simple_job_dict to agent
    agent_url = "http://localhost:8001"
    with xmlrpc.client.ServerProxy(agent_url) as agent:
        try:
            # test simple docker job
            simple_job_dict = {}
            simple_job_dict['job_id'] = 'id0'
            simple_job_dict['image_url'] = 'qizixi/barcode-generator:v0'
            simple_job_dict['resource_requirement'] = {'cpu':1, 'memory':1}
            simple_job_dict['resource_limit'] = {'cpu':1, 'memory':1}
            simple_job_dict['restart'] = True
            simple_job_dict['restart_times'] = 1
            submission_result = agent.submit_job(simple_job_dict)
            #print("submission_result:", submission_result)
            pulse_data = agent.heartbeat()
            print(pulse_data)
            agent_output = agent.stream_output(simple_job_dict['job_id'])
            print("agent_output:",agent_output)
            # print(type(agent_output))
            pulse_data = agent.heartbeat()
            print(pulse_data)
        except xmlrpc.client.Error as err:
            # todo: handle failure
            print("Error ", err)
            quit()