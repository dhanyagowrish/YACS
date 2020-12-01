# YACS
____________________________________________________________________________
Subject : Big Data (UE18CS322)
Project Title : Yet Another Centralized Scheduler
University : PES University, Bangalore
Authors : Ameya Bhamare, Atmik Ajoy, Chethan Mahindrakar, Dhanya Gowrish
____________________________________________________________________________

Steps for execution :
1. python master.py config.json <(RR, LL, RANDOM)>
2. python worker.py 4000 1
3. python worker.py 4001 2
4. python worker.py 4002 3
5. python requests.py <no_of_requests>