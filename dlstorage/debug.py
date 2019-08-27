"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

debug.py defines some primitives that are useful for debuging and evaluating
performance.
"""
from dlstorage.xform import VideoTransform
from dlstorage.filesystem.file import add_ext

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
class TestTagger(VideoTransform):
	def __init__(self):
		super(TestTagger, self).__init__()

	def _get_tags(self, img):
		tags = []

		for i in range(10):
			label = random.choice(string.ascii_lowercase)
			bb = (int(random.random()*self.vstream.width),
			      int(random.random()*self.vstream.height),
			      int(random.random()*self.vstream.width),
			      int(random.random()*self.vstream.height))
			tags.append((label, bb))

		return tags

	def __next__(self):
		out = next(self.input_iter)
		out['tags'] = self._get_tags(out['data'])
		return out