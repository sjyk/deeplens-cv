"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

utils.py defines some utilities that can be used for debugging and testing our system
"""

import json
import random
import string
import time
import os
from deeplens.utils.utils import add_ext
"""Calculates size of a directory
Stolen from https://stackoverflow.com/questions/1392413/calculating-a-directorys-size-using-python
"""
def get_size(start_path = '.'):
	total_size = 0
	for dirpath, dirnames, filenames in os.walk(start_path):
		for f in filenames:
			fp = os.path.join(dirpath, f)
			# skip if it is symbolic link
			if not os.path.islink(fp):
				total_size += os.path.getsize(fp)
	return total_size

"""Calculates the size of files on disk for a particular cached stream
"""
def sizeof(output):
    seq = 0
    size = 0
    
    while True:
        try:
            file = add_ext(output, '.seq', seq) 
            size += os.path.getsize(file)
            seq += 1
        
        except FileNotFoundError:
            break
    return size
    
"""Calculates the read time for a particular stream
"""
def timeof(vstreams):
    now = time.time()
    
    for vstream in vstreams:
        list(vstream) #materialize
    return (time.time() - now)

def printCrops(crops):
    print('~~~PRINTING CROPS~~~')
    for crop in crops:
        print("Crop: {}".format(crop['label']))
        print("bb: {}".format(crop['bb'].serialize()))

"""Types of gets: pipeline, video, query
"""
def testGet(manager, query, order = True, pipeline = None):
    pass

"""Types of gets: pipeline, video, query
"""
def testPut(maanger, splitter):
    pass
