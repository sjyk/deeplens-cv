"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

buffer_map.py defines the main adaptive dataflow component in DeepLens
"""

import cv2
from dlcv.utils import *
from dlcv.struct import Operator

import time
import numpy as np

class Map(Operator):
	"""Map is an abstract dataflow operator that applies a transformation
	frame by frame.
	"""

	def __init__(self):
		pass


	def __iter__(self):
		self.frame_iter = iter(self.video_stream)

		self.super_iter()
		return self

	def map(self, data):
		raise NotImplemented("Map must implement a map function")


	def __next__(self):
		frame = next(self.frame_iter)
		return self.map(frame)


class Crop(Map):

	def __init__(self,x0,y0,x1,y1):
		self.x0 = x0
		self.y0 = y0
		self.x1 = x1
		self.y1 = y1

	def map(self, data):
		ff = data.copy()
		ff['data'] = ff['data'][self.y0:self.y1,self.x0:self.x1]
		return ff


class Grayscale(Map):

	def map(self, data):
		ff = data.copy()
		ff['data'] = cv2.cvtColor(ff['data'], cv2.COLOR_BGR2GRAY)
		return ff


class Resize(Map):

	def __init__(self, scale):
		self.scale = scale

	def map(self, data):
		ff = data.copy()
		newX,newY = ff['data'].shape[1]*self.scale, ff['data'].shape[0]*self.scale
		ff['data'] = cv2.resize(ff['data'],(int(newX),int(newY))) 
		return ff
		

class Cut(Operator): 
	"""Cut is a video transformation that returns the clip that lies within a range 
	"""

	def __init__(self, start, end):
		self.cut_start = start
		self.cut_end = end

	def __iter__(self):
		"""__iter__() initializes the iterator. We use this steps to cache 
		height and width data.
		"""
		self.input_iter = iter(self.video_stream)
		self.super_iter()

		try:
			out = next(self.input_iter)

			while out['frame'] < self.cut_start:
				out = next(self.input_iter)

		except StopIteration:

			self.input_iter = iter([])


		return self


	def __next__(self):
		"""This implements the skipping logic for the cutting transformation
		"""
		out = next(self.input_iter)

		if out['frame'] <= self.cut_end:
			return out
		else:
			raise StopIteration()



class Sample(Operator): 
	"""Sample is a transform that drops frames from a video stream at a given 
	rate. 
	"""

	def __init__(self, ratio):
		"""Sample is constructed with a sampling ratio of 0-1, which denotes 
		the frequency of frames to skip.

		Args:
			ratio (float) - the frequency of frames to skip
		"""
		self.skip = int(1.0/ratio)


	def __iter__(self):
		self.frame_iter = iter(self.video_stream)
		self.super_iter()
		return self


	def __next__(self):
		"""This implements the skipping logic for the Sampling transformation
		"""
		out = next(self.frame_iter)
		if (self.video_stream.frame_count-1) % self.skip == 0:
			return out
		else:
			return self.__next__()



