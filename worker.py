# YACS PROJECT
# worker.py file

'''
Authors:
Ameya Bhamare           PES1201800351
Atmik Ajoy              PES1201800189              
Chethan Mahindrakar     PES1201801126
Dhanya Gowrish          PES1201800965

PES University
'''

# importing requied packages

import sys
import os
import json
import random
import socket
import threading
import ast
from datetime import *
import time
from time import *


class new_thread(threading.Thread):
    def __init__(self, thread_id, tf, param):
        threading.Thread.__init__(self)
        self.tf = tf
        self.param=param
        self.thread_id = thread_id
    
    def run(self):
        self.tf(self.param)

# -----------------------------------------------------------------------------------------------

class Task:
    def __init__(self, job_id, task_id, duration):
        self.job_id = job_id
        self.task_id = task_id
        self.duration = duration
    
    # check if the task is completed
    def is_completed(self):
        duration = self.duration
        return (duration == 0)
   
    # decrements duration by a second
    def decrement(self):
        self.duration -= 1

    # print paramters 
    def print_parameters(self):
        print("Job_id",self.job_id,"Task_id",self.task_id,"Duration:",self.duration)


# ------------------------------------------------------------------------------------------------

class Worker:
    def __init__(self, worker_id, port):

        # find the worker record in config file
        f = open(r'config.json') 
        data = json.load(f) 
    
        # Iterating through the json 
        worker_slots=0
        for i in data["workers"]:
                if(i["worker_id"]==worker_id):
                    worker_slots=i["slots"]
                    f.close()

        self.worker_id = worker_id
        self.port = port
        self.slots = worker_slots
        self.free_slots = worker_slots

        self.task_execution_pool = []

        for i in range(0,self.slots):
            self.task_execution_pool.append(0)


        with open("logs/worker"+str(worker_id)+".txt", "a+") as log_file:
            log_file.write(str(datetime.now()) + ":\tThe worker has been started -> [worker_id:{0}, port:{1}, slots:{2}]\n".format(self.worker_id,self.port,self.slots))
    
    # function to check whether the worker has FREE slots i.e whether a task can be scheduled on the worker
    def is_available(self):
        if (self.free_slots==0):
            return False
        else:
            return True

    # function to delete a task from the execution pool when its duration = 0 - increment the free slots of the worker by 1
    def delete_task(self, task_execution_pool_index):
        self.task_execution_pool[task_execution_pool_index] = 0
        self.free_slots += 1


    
    # function to launch the assigned task to a free slot - when worker receives the task
    def task_to_slot(self, task_record):
        task_t = Task(task_record["job_id"], task_record["task_id"], task_record["duration"])

        with open("logs/worker"+str(worker_id)+".txt", "a+") as log_file:
            log_file.write(str(datetime.now()) + ":\tExecution of task has started -> [job_id:{0}, task_id:{1}]\n".format(task_record["job_id"],task_record["task_id"]))
        
        # assign task to empty slot 
        for i in range(len(self.task_execution_pool)):
            if self.task_execution_pool[i] == 0:
                self.task_execution_pool[i] = task_t
                self.free_slots -= 1
                break

    # print paramters 
    def print_parameters(self):
        print("Worker_id",self.worker_id,"Port",self.port,"Slots:",self.slots,"Free_slots",self.free_slots)

    

 # ---------------------------------------------------------------------------------------------------------------------------------------


# function to listen to the tasks being assigned to it from Master 
def listen_for_tasks(worker):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with s:
        s.bind(("localhost", worker.port))
        s.listen(2048)
        while True:
            conn, addr = s.accept()
            
            with open("logs/worker"+str(worker_id)+".txt", "a+") as log_file:
                lock.acquire()
                log_file.write(str(datetime.now()) + ":\tIncoming connection has been established -> [host:{0}, port:{1}]\n".format(addr[0], addr[1]))
                lock.release()
            
            with conn:
                received_task_json = conn.recv(2048).decode()
                if received_task_json:
                    task = json.loads(received_task_json)

                    with open("logs/worker"+str(worker_id)+".txt", "a+") as log_file:
                        lock.acquire()
                        log_file.write(str(datetime.now()) + ":\tWorker received task -> [job_id:{0}, task_id:{1}, duration:{2}]\n".format(task["job_id"], task["task_id"], task["duration"]))
                        lock.release()

                    lock.acquire()
                    worker.task_to_slot(task)
                    lock.release()

# --------------------------------------------------------------------------------------------------------------------------------------------------       
        
# function to take a slot - check if it has a task - if the task is over i.e duration is 0 UPDATE master
# if task is not over i.e duration is not 0 - decrement the duration by 1 second
def task_executor(worker):
    while True:
        if not worker.slots: 
            continue

        for i in range(worker.slots):
            if not worker.task_execution_pool[i]:   # implies that no task has been assigned to this slot
                continue

            elif (worker.task_execution_pool[i].is_completed()):    # implies that duration is over i.e task is completed
                task_exec = worker.task_execution_pool[i]
                job_id = task_exec.job_id
                task_id = task_exec.task_id
            
                with open("logs/worker"+str(worker_id)+".txt", "a+") as log_file:
                    lock.acquire()
                    log_file.write(str(datetime.now()) + ":\tWorker has finished executing task -> [job_id:{0}, task_id:{1}]\n".format(job_id, task_id))
                    lock.release()

                # delete the task - since the duration is 0
                lock.acquire()
                worker.delete_task(i)
                lock.release()
                
                finished_task_message = json.dumps({ "worker_id": worker.worker_id, "job_id": job_id, "task_id": task_id})

                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                with s:
                    s.connect(('localhost', 5001))
                    with open("logs/worker"+str(worker_id)+".txt", "a+") as log_file:
                        lock.acquire()
                        log_file.write(str(datetime.now()) + ":\tOutgoing connection has been established -> [host:{0}, port:{1}]\n".format("localhost", "5001"))
                        lock.release()
                    s.send(finished_task_message.encode())

                with open("logs/worker"+str(worker_id)+".txt", "a+") as log_file:
                    lock.acquire()
                    log_file.write(str(datetime.now()) + ":\tUpdate has been sent to master -> [job_id:{0}, task_id:{1}]\n".format(job_id, task_id))
                    lock.release()
            
            
            else:                           # implies the slot has a task but the task has not been completed i.e duration not equal to 0
                lock.acquire()
                worker.task_execution_pool[i].decrement()
                lock.release()
        
        sleep(1)
  
# ----------------------------------------------------------------------------------------------------------------------------------


if __name__ == "__main__":

    lock = threading.Lock()

    port = int(sys.argv[1])
    worker_id = int(sys.argv[2])


    # create worker
    worker = Worker(worker_id, port)        
 
    # creating logs folder and log file for the worker
    try:
        os.mkdir('logs')
    except:
        pass
    
    f = open("logs/worker"+str(worker_id)+".txt", "w")
    f.close()
   

    t1 = new_thread(1, listen_for_tasks, worker)
    t2 = new_thread(2, task_executor, worker)
 
    t1.start()
    t2.start()
    t1.join()
    t2.join()
