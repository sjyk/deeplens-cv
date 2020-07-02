"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

testing_utils.py defines some primitives that are useful for debugging and evaluating
performance.
"""
from deeplens.utils.utils import add_ext
from deeplens.pipeline import *

import json
import random
import string
import time
import os

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

"""Creates dummy tags without having to execute a neural
network library.
"""
class TestTagger(Operator):
    def __init__(self):
        super(TestTagger, self).__init__()
        
    def _get_tags(self):
        tags = []
        
        for i in range(10):
            label = random.choice(string.ascii_lowercase)
            bb = (int(random.random()*self.video_stream.width),
                    int(random.random()*self.video_stream.height),
                    int(random.random()*self.video_stream.width),
                    int(random.random()*self.video_stream.height))
            tags.append((label, bb))
            
        return tags
        
    def __next__(self):
        out = next(self.input_iter)
        out['tags'] = self._get_tags()
        return out

""" Creates dummy splits/crops without optimization
"""
class TestSplitter(Operator):
    def __init__(self):
        super(TestSplitter, self).__init__()
        
    def __next__(self):
        out = next(self.input_iter)
        if out['frame']%50 == 0:
            cr = (0,0, int(0.5*self.video_stream.width), int(0.5*self.video_stream.height))
            out['crop'] = [cr] # denote a list of crops if present
            out['split'] = True # split the video at this point
        else:
            out['crop'] = [] # denote a list of crops if present
            out['split'] = False # split the video at this point
            return out

class CustomTagger(Operator):
	def __init__(self, tagger, batch_size):
		super(CustomTagger, self).__init__()
		# a custom tagger function that takes video_stream and batch_size; it raises StopIteration when finishes
		self.tagger = tagger
		self.batch_size = batch_size
		self.next_count = 0  # how many next() we have called after _get_tags()
		self.stream = False
		self.frames = []

	def __iter__(self):
		self.input_iter = iter(self.video_stream)
		self.super_iter()
		return self

	def _get_tags(self):
		if self.next_count == 0 or self.next_count >= self.batch_size:
			self.next_count = 0
			# we assume it iterates the entire batch size and save the results
			self.tags = []
			try:
				if self.stream:
					tag, frames = self.tagger(self.input_iter, self.batch_size, video = True)
					self.frames = frames
				else:
					tag = self.tagger(self.input_iter, self.batch_size)
			except StopIteration:
				raise StopIteration("Iterator is closed")
			if tag:
				self.tags.append(tag)

		self.next_count += 1
		return self.tags

	def __next__(self):
		if self.stream:
			tags = self._get_tags()
			return {'objects': tags, 'frame': self.frames[self.next_count - 1]}
		else:
			return {'objects': self._get_tags()}

	def set_stream(self, stream):
		self.stream = stream

class Serializer(json.JSONEncoder):
	def default(self, obj):
		return obj.serialize()
