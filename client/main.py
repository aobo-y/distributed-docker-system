import xmlrpc.client
from pathlib import Path
import yaml
import os

proxy = None

def run(input_master_url):
    global proxy
    proxy = xmlrpc.client.ServerProxy("http://" + master_url)
    try:
        proxy.is_even(0)                                            # check if server is up
        print("connection established...")
        if not os.path.exists("./tickets.txt"):                     # store tickets
            os.mknod("./tickets.txt")
        print("ticket file created...")
        return True
    except xmlrpc.client.ProtocolError as err:
        print("error message: %s" % err.errmsg)
        return False
    except xmlrpc.client.Fault as err:
        print("error message: %s" % err.faultString)
        return False

def load_tickets():
    tickets = []
    if os.path.exists("./tickets.txt"):
        ticket_file = open("./tickets.txt")
        for l in ticket_file.readlines():
            tickets.append(str(l).rstrip('\n'))
        ticket_file.close()
    return tickets

def insert_ticket(job_id):
    tickets = load_tickets()
    if job_id not in tickets:
        ticket_file = open("./tickets.txt", 'a')
        ticket_file.write("%s\n" % job_id)
    ticket_file.close()

def get_status(job_id):
    global proxy
    try:
        tickets = load_tickets()
        if job_id in tickets:
            print(proxy.get_status(job_id))
        else:
            print("invalid job_id...")
    except xmlrpc.client.Fault as err:
        print("error message: %s" % err.faultString)

def list_jobs():
    tickets = load_tickets()
    print("id of ongoing jobs: ")
    for ticket in tickets:
        print(ticket)

def stream_output(job_id):
    pass

def submit_job(path):
    global proxy
    job_file = Path(path)
    if job_file.is_file():
        job_file = open(path)
        job_dict = yaml.safe_load(job_file)
        try:
            job_id = proxy.submit_jobs(job_dict)
            insert_ticket(job_id)
            print("submission succeeded, job id: %s" % job_id)
        except xmlrpc.client.Fault as err:
            print("error message: %s" % err.faultString)
    else:
        print("path invalid...")

def cmd_switch(cmd):
    if cmd[0] == "submit_job":
        job_description_path = cmd[1]
        submit_job(job_description_path)
    elif cmd[0] == "get_status":
        job_id = cmd[1]
        get_status(job_id)
    elif cmd[0] == "kill_job":
        job_id= cmd[1]
        kill_job(job_id)
    elif cmd[0] == "list_jobs":
        list_jobs()
    elif cmd[0] == "stream_output":
        job_id = cmd[1]
        stream_output(job_id)
    else:
        print("command not recognized...")

if __name__ == "__main__":
    try:
        connected = False
        while not connected:
            master_url = input("master_url: ")
            if master_url == "" or master_url == '\n':
                continue
            connected = run(master_url)
        while True:
            cmd = input(">: ")
            if cmd == "" or cmd == '\n':
                continue
            else:
                cmd = cmd.split(" ")
                cmd_switch(cmd)
    except KeyboardInterrupt:
        print("interrupted. aborting.")