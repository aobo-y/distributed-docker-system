from xmlrpc.server import SimpleXMLRPCServer
from random import randint

free_id = set(map(str, range(0, 1000)))
jobs = {}

def is_even(n):
    return n % 2 == 0

def submit_jobs(input_job_dict):
	global free_id 
	global jobs
	job_id = free_id.pop()
	jobs[job_id] = input_job_dict
	jobs[job_id]['status'] = "running"
	return job_id

def get_status(job_id):
	return jobs[job_id]['status']

def kill_job(job_id):
	pass

server = SimpleXMLRPCServer(("localhost", 8000))
print("Listening on port 8000...")
server.register_function(is_even, "is_even")
server.register_function(submit_jobs, "submit_jobs")
server.register_function(get_status, "get_status")
server.serve_forever()


