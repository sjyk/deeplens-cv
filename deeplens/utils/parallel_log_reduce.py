"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

paralleL_log_reduce.py calculates approximate intermidiate times for put

""" 
import json
import os

def paralleL_log_reduce(logs, start_time):
    times = []
    for log in logs:
        with open(log, 'r') as f:
            time = json.load(f)
            time['end_time'] = time['end_time'] - start_time
            times.append(time['end_time'])
    times.sort()
    return times

def paralleL_log_delete(logs):
    for log in logs:
        os.remove(log)