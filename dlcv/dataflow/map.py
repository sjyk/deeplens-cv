"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

map.py defines the simplest dataflow component in DeepLens
"""
import random

import cv2
from dlcv.utils import *
from dlcv.struct import Operator

import time
import numpy as np

class Map(Operator):
	"""Map is an abstract dataflow operator that applies a transformation
	frame by frame.
	"""

	#dummy constructor that is overridden by inherriting classes
	def __init__(self):
		pass

	#subclasses must implement this
	def map(self, data):
		raise NotImplemented("Map must implement a map function")


	#each operator can be told to crop itself, upto subclasses to figure out
	#how to use this.
	def setCrop(self, crop):
		self.crop = crop

	#sets up a generic iterator
	def __iter__(self):
		self.frame_iter = iter(self.video_stream)
		self.super_iter()
		return self

	def __next__(self):
		frame = next(self.frame_iter)
		self.super_next()

		try:
			frame_copy = frame.copy()
			frame_copy['data'] = bb_crop(frame_copy['data'], self.crop)
			frame_copy = self.map(frame_copy)
			frame_copy['data'] = bb_replace(frame['data'], self.crop, frame_copy['data'])
			return frame_copy
		except:
			return self.map(frame)



class Crop(Map):
	"""The Crop() operator crops all future frames to the given bounding box
	"""

	def __init__(self,x0,y0,x1,y1):
		self.x0 = x0
		self.y0 = y0
		self.x1 = x1
		self.y1 = y1

	def map(self, data):
		ff = data
		ff['data'] = ff['data'][self.y0:self.y1,self.x0:self.x1]
		return ff

	def _serialize(self):
		return {'x0': self.x0,
				'y0': self.y0,
				'x1': self.x1,
				'y1': self.y1}


class Grayscale(Map):
	"""The GrayScale() operator sets all future frames to be grayscale
	"""

	def map(self, data):
		ff = data
		ff['data'] = cv2.cvtColor(ff['data'], cv2.COLOR_BGR2GRAY)
		return ff


	def _serialize(self):
		return {}


class Resize(Map):
	"""The Resize operator takes in a number that scales the frames up or
	down by that factor.
	"""

	def __init__(self, scale):
		self.scale = scale

	def map(self, data):
		ff = data
		newX,newY = ff['data'].shape[1]*self.scale, ff['data'].shape[0]*self.scale
		ff['data'] = cv2.resize(ff['data'],(int(newX),int(newY))) 
		return ff

	def _serialize(self):
		return {'scale': self.scale}
		

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
		self.super_next()

		if out['frame'] <= self.cut_end:
			return out
		else:
			raise StopIteration()


	def _serialize(self):
		return {'start': self.start, 'end': self.end}



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
		self.super_next()

		if (self.video_stream.frame_count-1) % self.skip == 0:
			return out
		else:
			return self.__next__()


	def _serialize(self):
		return {'ratio': self.ratio}



class SampleClip(Operator):
	"""SampleClip is a transform that drops clips from a video stream at given
	clip_size and (1-prob_keep) probability.
	"""

	def __init__(self, clip_size, prob_keep):
		"""
		SampleClip is constructed with a clip size and a probability of keeping
		the clip.

		Args:
		    clip_size (int) - the size of each clip
		    prob_keep (float) - the probability of keeping a clip
		"""
		assert prob_keep > 0 and prob_keep <= 1
		self.clip_size = clip_size
		self.prob_keep = prob_keep
		self.counter = 0
		self.hit = False

	def __iter__(self):
		self.frame_iter = iter(self.video_stream)
		self.super_iter()
		return self

	def __next__(self):
		"""This implements the skipping logic for the SampleClip transformation
		"""
		out = next(self.frame_iter)
		self.super_next()
		if self.counter >= self.clip_size:
			self.hit = False
			self.counter = 0

		if self.hit == False:
			if random.random() <= self.prob_keep:
				# keep this clip
				self.hit = True
				self.counter = 0
			else:
				# skip this clip
				for _ in range(self.clip_size - 1):
					_ = next(self.frame_iter)

		if self.hit:
			self.counter += 1
			return out

		return self.__next__()

	def _serialize(self):
		return {'clip_size': self.clip_size,
		        'prob_keep': self.prob_keep}


