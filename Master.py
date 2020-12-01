# importing required packages

import sys
import os
import json
import random
import socket
import threading
import operator
import pprint
import ast
from datetime import *
from _thread import *


# SCHEDULING ALGORITHMS
# function for Random Scheduling 
def RS():
    while True:
        worker_id = random.randint(1, 3)
        if free_slots[worker_id]['slots'] > 0:
                return worker_id

# function for Round Robin Scheduling
def RR():
    while True:
        worker_id=1
        while worker_id < 4:
            if free_slots[worker_id]['slots'] > 0:
                    return worker_id
            worker_id+=1

# function for Least Loaded Scheduling              
def LL():
    min_worker_id = 1
    worker_id=2
    while worker_id < 4:
        if free_slots[worker_id]['slots'] > free_slots[min_worker_id]['slots']:
            min_worker_id = worker_id

        worker_id+=1
    return min_worker_id

# -----------------------------------------------------------------------------------------------------

class new_thread(threading.Thread):
    def __init__(self, thread_id, tf):
        threading.Thread.__init__(self)
        self.thread_id = thread_id
        self.tf = tf
    
    def run(self):
        self.tf()

# ---------------------------------------------------------------------------------------------------

# function to schedule worker based on scheduling algorithm passed
def assigned_worker(scheduling_algorithm):
    schedulers = {"RANDOM":RS(), "RR":RR(), "LL":LL()}

    if(scheduling_algorithm not in schedulers):
        return -1
    return schedulers[scheduling_algorithm]


#SENDS THE TASK TO THE APPROPRIATE WORKER      
def scheduling_tasks():    
    scheduling_algorithm = sys.argv[2]

    while(True):
        if(len(tasks_queue) > 0):
            for task in tasks_queue:
                worker_id = assigned_worker(scheduling_algorithm)

                if(worker_id==-1):
                    print("Enter correct scheduling algorithm! Either RANDOM or RR or LL")
                    sys.exit()
                
                
                # assigned worker's port
                port = free_slots[worker_id]['port']
                

                task_to_schedule = tasks_queue[0]
                tasks_queue.remove(task_to_schedule)
                message = json.dumps(task_to_schedule)
                
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect(("localhost", port))

                    with open("logs/master.txt", "a+") as f:
                        lock.acquire()
                        f.write(str(datetime.now()) + "\t\t OUTGOING connection has been established : (host:{0}, port:{1}) -- \n".format("localhost", port))
                        lock.release()
                    s.send(message.encode())

                with open("logs/master.txt", "a+") as f:
                    lock.acquire()
                    f.write(str(datetime.now()) + "\t\t Worker has been sent task : -- worker_id:{0}, job_id:{1}, task_id:{1}, duration:{1} -- \n".format(worker_id, task_to_schedule['job_id'], task_to_schedule['task_id'], task_to_schedule['duration']))
                    lock.release()
                
                print("Sent task", task_to_schedule['task_id'], "to the worker", worker_id, "on the port", port)
                
                # acquiring lock to DECREMENT the free slots in that worker
                lock.acquire()
                free_slots[worker_id]['slots'] -= 1
                lock.release()
    
# ------------------------------------------------------------------------------------------------------------------------------------------------  
  
#listen for jobs from requests.py
def listen_for_jobs():
    host = ""
    port = 5000
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)


    with s:
        s.bind((host, port))        # binds port 5000 to hosts own IP address
        s.listen(500)

        while(1):
            conn, addr = s.accept()

            with open("logs/master.txt", "a+") as log_file:
                lock.acquire()
                log_file.write(str(datetime.now()) + "\t\tINCOMING connection has been established: (host:{0}, port:{1})\n".format(addr[0], addr[1]))
                lock.release()
            
            with conn:
                job = conn.recv(2048)
                job = json.loads(job)

                map_task_id =[]
                for i in job['map_tasks']:
                    map_task_id.append(i['task_id'])

                reduce_task_id = []

                for i in job['reduce_tasks']:
                    reduce_task_id.append(i['task_id'])
                    
                with open("logs/master.txt", "a+") as log_file:
                    lock.acquire()
                    log_file.write(str(datetime.now()) + "\t\t Job request has been RECEIVED = [job_id:{0}, map_tasks_ids:{1}, reduce_tasks_ids:{2}]\n".format(job['job_id'], map_task_id, reduce_task_id))
                    lock.release()

                
                # add number of map tasks to be executed in the job to the map_counts dictionary
                map_counts[job['job_id']] = {}
        
                count_map_tasks = len(job['map_tasks'])
                map_counts[job['job_id']] = count_map_tasks 
                
                # add number of tasks in the job to be executed to the jt_count dictionary
                m= len(job['map_tasks'])
                r = len(job['reduce_tasks'])
                jt_count[job['job_id']] = m + r


                # reduce tasks to be scheduled on workers after execution of ALL map tasks in the job
                r_task = {}
                reduce_tasks_per_job[job['job_id']] = []
                temp = job['reduce_tasks']

                for ele in temp:
                    t_id=ele['task_id']
                    r_task[t_id] = {}
                    r_task[t_id]['task_id'] = ele['task_id'] 
                    r_task[t_id]['job_id'] = job['job_id']
                    r_task[t_id]['duration'] = ele['duration']
                    reduce_tasks_per_job[job['job_id']].append(r_task[t_id])
                
                
                # add map tasks to be scheduled on workers to task queue 
                m_task = {}
                temp = job['map_tasks']
                
                for ele in temp:
                    t_id=ele['task_id']
                    m_task[t_id] = {}
                    m_task[t_id]['task_id'] = ele['task_id'] 
                    m_task[t_id]['job_id'] = job['job_id']
                    m_task[t_id]['duration'] = ele['duration']
                    tasks_queue.append(m_task[t_id])
                
# -----------------------------------------------------------------------------------------------------------------------------------

# function to listen to updates from workers - i.e when workers finish jobs
def listen_for_worker_updates():

    host = ""
    port = 5001

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    with s:
        s.bind((host, port))
        s.listen(500)
        while(True):
            conn, addr = s.accept()

            with open("logs/master.txt", "a+") as log_file:
                lock.acquire()
                log_file.write(str(datetime.now()) + "\t\tINCOMING connection has been established: (host:{0}, port:{1})\n".format(addr[0], addr[1]))
                lock.release()

            with conn:
                worker_update_json = conn.recv(1024)
                worker_update = json.loads(worker_update_json)

                worker_id = worker_update['worker_id']
                job_id = worker_update['job_id']
                task_id = worker_update['task_id']

                # writing above details into LOG file
                with open("logs/master.txt", "a+") as log_file:
                    lock.acquire()
                    log_file.write(str(datetime.now()) + "\t\tUPDATE has been received from worker: (worker_id:{0}, task_id:{1}) - Task completed\n".format(worker_id, task_id))
                    lock.release()

                print("Task", task_id, "has completed execution in", worker_id, "worker")
                
                # decrement number of remaining tasks in job
                jt_count[job_id] -= 1

                if jt_count[job_id] == 0:
                    with open("logs/master.txt", "a+") as log_file:
                        lock.acquire()
                        log_file.write(str(datetime.now()) + "\t\tJob (job_id:{0}) has FINISHED execution\n".format(job_id))
                        lock.release()

                # increment the number of free slots for the worker
                lock.acquire()
                free_slots[worker_id]['slots'] += 1
                lock.release()
                
                # if a Map task is completed and ALL map tasks in the job are completed - then add reduce tasks to task queue to be scheduled
                # identify map tasks by 'M' in the task_id
                if 'M' in task_id: 
                    map_counts[job_id] -= 1
                    if map_counts[job_id] == 0:
                        for reduce_task in reduce_tasks_per_job[job_id]:
                            tasks_queue.append(reduce_task)
                        
                    
# ------------------------------------------------------------------------------------------------------------------------------------------

    
if __name__ == '__main__':
    map_counts = {}                 #counts of remaining map tasks per job that are left to be executed
    jt_count = {}                   #counts of tasks per job that are left to be executed
    reduce_tasks_per_job = {}       #list of reduce tasks per job
    tasks_queue = []                #list of tasks to be scheduled

    lock = threading.Lock()

    try:
        os.mkdir('logs')
    except:
        pass 
    
    f = open("logs/master.txt", "w")
    f.close()
    path_to_config = sys.argv[1]
    with open(path_to_config) as f:
        config = json.load(f)
    
    
                                       # print the free slots details for all workers
    free_slots = {}     
    for worker in config['workers']:
        free_slots[worker['worker_id']] = {'slots':worker['slots'], 'port':worker['port']}
    print("Worker details:")
    print(free_slots)
    
    
    t1 = new_thread(1, listen_for_jobs)
    t2 = new_thread(2, listen_for_worker_updates)
    t3 = new_thread(3, scheduling_tasks)

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()
  
