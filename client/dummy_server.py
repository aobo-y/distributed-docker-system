from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
from random import randint

free_id = set(map(str, range(0, 1000)))
jobs = {}

def is_even(n):
    return n % 2 == 0

def submit_job(input_job_dict):
	global free_id 
	global jobs
	job_id = free_id.pop()
	jobs[job_id] = input_job_dict
	jobs[job_id]['status'] = "running"
	return job_id

def get_status(job_id):
    if job_id not in jobs:
        raise xmlrpc.client.Fault(1, 'job id not exist')
    else:
        return jobs[job_id]['status']

def kill_job(job_id):
	return True

def output_request(job_id):
	return xmlrpc.client.Binary(bytes([0]))

server = SimpleXMLRPCServer(("localhost", 8000))
print("Listening on port 8000...")
server.register_function(is_even, "is_even")
server.register_function(submit_job, "submit_job")
server.register_function(get_status, "get_status")
server.register_function(kill_job, "kill_job")
server.register_function(output_request, "output_request")
server.serve_forever()


