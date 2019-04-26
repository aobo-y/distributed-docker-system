import xmlrpc.client
import yaml
import os
import socket
from tabulate import tabulate

proxy = None

class JobDictFormatError(Exception):
    def __init__(self):
        pass

def run(input_master_url):
    global proxy
    proxy = xmlrpc.client.ServerProxy("http://" + master_url)
    try:
        proxy.is_even(0)                     
    except xmlrpc.client.ProtocolError as err:
        print("xmlrpc.client.ProtocalError: %s" % err.errmsg)
        return False
    except xmlrpc.client.Fault as err:
        print("xmlrpc.client.Fault: %s" % err.faultString)
        return False
    except ConnectionRefusedError as err:
        print("ConnectionRefusedError: connection refused...")
        return False
    except socket.gaierror as err:
        print("socket.gaierror: name or service not known...")
        return False
    else:
        if not os.path.exists("./tickets.txt"):
            os.mknod("./tickets.txt")
        print("ticket file created...")
        return True

def job_dict_valid(job_dict):
    return ("img_url" in job_dict 
            and "resource_requirement" in job_dict 
            and "cpu" in job_dict["resource_requirement"]
            and isinstance(job_dict["resource_requirement"]["cpu"], int)
            and "resource_limit" in job_dict
            and "cpu" in job_dict["resource_limit"]
            and isinstance(job_dict["resource_limit"]["cpu"], int)
            and "restart" in job_dict
            and "restart_times" in job_dict)

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
    ticket_file = open("./tickets.txt", 'a+')
    ticket_file.write("%s\n" % job_id)
    ticket_file.close()

def delete_ticket(job_id):
    tickets = load_tickets()
    if job_id in tickets:
        open('./tickets.txt', 'w').close()
        tickets.remove(job_id)
        for ticket in tickets:
            insert_ticket(ticket)

def kill_job(job_id):
    tickets = load_tickets()
    if job_id in tickets:
        try:
            global proxy
            killed_by_master = proxy.kill_job(job_id)
        except xmlrpc.client.ProtocolError as err:
            print("xmlrpc.client.ProtocalError: %s" % err.errmsg)
        except xmlrpc.client.Fault as err:
            print("xmlrpc.client.Fault: %s" % err.faultString)
        else:
            if killed_by_master:
                delete_ticket(job_id)
                print("job killed...")
            else:
                print("master unable to kill the job")
    else:
        print("job_id invalid")

def get_status(job_id):
    status = ""
    try:
        global proxy
        status = proxy.get_status(job_id)
    except xmlrpc.client.ProtocolError as err:
        print("xmlrpc.client.ProtocalError: %s" % err.errmsg)
    except xmlrpc.client.Fault as err:
        print("xmlrpc.client.Fault: %s" % err.faultString)
    finally:
        return status

def list_jobs():
    tickets = load_tickets()
    table = []
    for ticket in tickets:
        status = get_status(ticket)
        table.append([ticket, status])
    print("")
    print(tabulate(table, headers=['Job ID', 'Status'], tablefmt='orgtbl'))

def stream_output(job_id):
    tickets = load_tickets()
    if job_id in tickets:
        global proxy
        try:
            output = proxy.output_request(job_id)
        except xmlrpc.client.ProtocolError as err:
            print("xmlrpc.client.ProtocalError: %s" % err.errmsg)
        except xmlrpc.client.Fault as err:
            print("xmlrpc.client.Fault: %s" % err.faultString)
        else:
            output_file = open("./job_" + job_id + "_output.txt", 'wb+')
            output_file.write(output.data)
    else:
        print("job_id invalid")

def submit_job(job_file_path):
    try:
        global proxy
        job_file = open(job_file_path)
        job_dict = yaml.safe_load(job_file)
        if not job_dict_valid(job_dict):
            raise JobDictFormatError
        job_id = proxy.submit_job(job_dict)
    except FileNotFoundError as err:
        print("No such file '%s'" % job_file_path)
    except xmlrpc.client.ProtocolError as err:
        print("xmlrpc.client.ProtocalError: %s" % err.errmsg)
    except xmlrpc.client.Fault as err:
        print("xmlrpc.cleint.Fault: %s" % err.faultString)
    except JobDictFormatError as err:
        print("job dict format error")
    else:
        print("submission succeeded. job id : %s" % job_id)
        insert_ticket(job_id)

def cmd_switch(cmd):
    if cmd[0] == "submit_job":
        try:
            job_file_path = cmd[1]
        except IndexError:
            print("Error: missing argument")
        else:
            submit_job(job_file_path)
    elif cmd[0] == "list_jobs":
        list_jobs()
    elif cmd[0] == "stream_output":
        try:
            job_id = cmd[1]
        except IndexError:
            print("Error: missing argument")
        else:
            stream_output(job_id)
    elif cmd[0] == "kill_job":
        try:
            job_id= cmd[1]
        except IndexError:
            print("Error: missing argument")
        else:
            kill_job(job_id)
    else:
        print("%s : command not found..." % cmd[0])

if __name__ == "__main__":
    try:
        connected = False
        while not connected:
            master_url = input("master_url: ")
            if master_url == "" or master_url == '\n':
                continue
            else:
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