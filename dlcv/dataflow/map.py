"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

map.py defines the simplest dataflow component in DeepLens
"""

import cv2
from dlcv.utils import *
from dlcv.struct import Operator, Box
import math
import time
import numpy as np
from random import randint

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
		if (self.video_stream.frame_count-1) % self.skip == 0:
			return out
		else:
			return self.__next__()


	def _serialize(self):
		return {'ratio': self.ratio}


class RUS(Operator): 
	"""Sample is a transform that drops frames from a video stream at a given 
	rate. 
	"""

	def __init__(self, upper_limit):
		"""
		Args:
			
		"""
		self.upper_limit = upper_limit
		self.value = 0
		


	def __iter__(self):
		self.frame_iter = iter(self.video_stream)
		self.super_iter()
		return self


	def __next__(self):
		"""This implements the skipping logic for the Sampling transformation
		"""
		
		out = next(self.frame_iter)
		if self.value != 0:
			self.value -=1
			return self.__next__()
		else:
			self.value = randint(0,self.upper_limit)
			return out 


class EBGS(Operator):
	"""EBGStop (Empirical Bernstein Stopping) as in the paper:
	Volodymyr Mnih, Csaba Szepesvári, and Jean-Yves Audibert. 2008. Empirical Bernstein stopping. 
	In Proceedings of the 25th international conference on Machine learning (ICML '08). 
	ACM, New York, NY, USA, 672-679. DOI=http://dx.doi.org/10.1145/1390156.1390241
	"""
	def __init__(self, epsilon, delta, variable_range):
		"""EBGS is an adaptative sampling algorithm that uses a geometric sampling schedule 
		with a near-optimal stopping rule for relative error estimation on bounded random variable

		Args:
			epsilon (float): 
			delta (float): user definided confidence, i.e., the event happens with 
			probability at least (1-delta)
			variable_range (float): range of the variable being measured
		"""
		self.epsilon = epsilon
		self.delta = delta
		self.range = variable_range
		self.t = 1
		self.k = 0
		self.upper_bound = 10000000000000
		self.lower_bound = -1000000000000
		self.beta = 1.5
		self.p = 1.1
		self.filter="object"
		self.c = self.delta * (self.p-1)/self.p
		self.indicator = []
		self.region = Box(200,550,350,750)
		self.last = None


	def __iter__(self):
		self.frame_iter = iter(self.video_stream)
		self.super_iter()
		return self


	def __next__(self):
		"""This implements the logic for the EBGStop sampling transformation
		"""
		out = next(self.frame_iter)


		#Checking for stopping condition
		if (self.epsilon + 1) * self.lower_bound < (1 - self.epsilon) * self.upper_bound:
			self.t += 1
			if self.last == None:
				pass
			elif 'bounding_boxes' in self.last:
			#print('number of bounding boxes:',len(self.last['bounding_boxes']))
				cnt = 0
				for label, pt in self.last['bounding_boxes']:
					box = Box(*pt)
					if label == self.filter and self.region.contains(box):
						cnt += 1
					else:
						pass
				if cnt == 0:
					self.indicator.append(0)
					
				else:
					#print('1')
					self.indicator.append(1)
			
			#Adding new value

			if self.t > np.floor(self.beta ** self.k):
				self.k += 1
				self.alpha = np.floor(self.beta ** self.k) / np.floor(self.beta ** (self.k - 1))
				self.dk = self.c / (0.00000000000001 + (math.log(self.k, self.p) ** self.p))
				self.x = -self.alpha * np.log(self.dk) / 3

			values = np.asarray(self.indicator)
			ct = np.std(values)* np.sqrt(2 * self.x / self.t) + 3 * self.range * self.x / self.t
			self.lower_bound = max(self.lower_bound, np.abs(np.mean(values))-ct)
			self.upper_bound = min(self.upper_bound, np.abs(np.mean(values))+ct)
		

			self.last = out
			return out
		else:
			return self.__next__()


	def _serialize(self):
		return {'epsilon': self.epsilon, 'delta': self.delta, 'range': self.range}



