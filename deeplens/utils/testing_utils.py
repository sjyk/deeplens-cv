"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

testing_utils.py defines some primitives that are useful for debugging and evaluating
performance.
"""
from deeplens.struct import Operator
from deeplens.simple_manager.file import add_ext

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


"""Creates dummy tags without having to execute a neural
network library.
"""
class TestTagger(Operator):
	def __init__(self):
		super(TestTagger, self).__init__()

	def __iter__(self):
		self.input_iter = iter(self.video_stream)
		self.super_iter()
		return self

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

	def __iter__(self):
		self.input_iter = iter(self.video_stream)
		self.super_iter()
		return self

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