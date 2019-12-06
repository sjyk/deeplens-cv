"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

buffer_map.py defines the main adaptive dataflow component in DeepLens. It 
leverages SIMD operations when possible. 
"""
import logging

import cv2
from dlcv.utils import *
from dlcv.struct import Operator

import time
import numpy as np

class BufferMap(Operator):
	"""BufferMap is an abstract dataflow operator that buffers frames from a 
	stream and applies a function to the buffer.
	"""

	def __init__(self,
				 buffer_size=10, \
				 sampling_rate=1, \
				 resolution=1,
				 alpha=1,
				 ssthresh=300):
		"""buffer_map takes in three parameters: 
		the size of the buffer, a sampling rate, and a
		resolution resizing.
		"""

		self.buffer_size = buffer_size
		self.skip = int(1.0/sampling_rate)
		self.resolution = resolution
		self.alpha = alpha  # the alpha in exponentially-weighted moving average
		self.ssthresh = ssthresh  # slow start threshold
		self.max_buffer_size = -1  # when max rate is reached, ssthresh is fixed

	def __iter__(self):
		self.frame_iter = iter(self.video_stream)
		self.buffer = []
		self.output_dict = []
		self.buffer_state = 0
		self.rate = 0.0

		self.super_iter()
		return self

	#this is the main routine which buffers up the frames and applies a map function
	#to each buffer
	def _map(self):
		prediction_indices = range(0, min(self.buffer_size, len(self.buffer)), self.skip)
		inp = [self.buffer[p] for p in prediction_indices]


		if self.resolution == 1:
			output = self.map([frame['data'] for frame in inp])
		else:
			output = self.map([cv2.resize(frame['data'],
										  (int(frame['data'].shape[1]*self.resolution),\
										   int(frame['data'].shape[0]*self.resolution))) \
									 for frame in inp])


		self.output_dict = list(self.buffer)#copy

		for i,_ in enumerate(self.output_dict):
			self.output_dict[i].update(output[i])

		for i,pred in enumerate(prediction_indices):
			self.output_dict[pred].update(output[i])

		#self.updateBatch()


	"""map() must be implemented by each subclass.
	"""
	def map(self, data):
		raise NotImplemented("BufferMap must implement a map function")


	def _tune_buffer_size(self, rate, add_factor=50, multiply_factor=0.7):
		# slow start
		if self.buffer_size < self.ssthresh:
			self.buffer_size *= 2

		# additive-increase/multiplicative-decrease (AIMD)
		if self.max_buffer_size == -1 and rate < self.rate:
			self.ssthresh = int(self.buffer_size / 2)
			self.buffer_size = int(self.buffer_size * multiply_factor) + 1
			self.max_buffer_size = self.buffer_size
		elif self.max_buffer_size != -1 and self.buffer_size > self.max_buffer_size and rate < self.rate:
			self.buffer_size = int(self.buffer_size * multiply_factor) + 1
		else:
			self.buffer_size = int(self.buffer_size + add_factor)



	#fill up the buffer
	def _fill_buffer(self):

		for i in range(self.buffer_size):
			frame = next(self.frame_iter, None)

			if frame == None:
				break

			self.buffer.append(frame)

		if len(self.buffer) > 0:
			logging.debug("Frame No. %s", self.buffer[0]['frame'])

	#iterator producer
	def __next__(self):
		self.super_next()

		if self.buffer_state == 0:
		#buffer is filled so run map first time
			logging.debug("buffer_state: 0. buffer_size: %s", self.buffer_size)
			now = time.time()
			self._fill_buffer()

			if len(self.buffer) == 0:
				raise StopIteration()

			self._map()
			delta = time.time() - now
			logging.debug("Time taken for _fill_buffer and _map: %s", delta)
			logging.debug("Average rate before: %s", self.rate)

			rate = self.buffer_size / delta
			self._tune_buffer_size(rate)

			logging.debug("Rate this time: %s", rate)

			# exponentially-weighted moving average
			if self.rate == 0:
				self.rate = rate
			else:
				self.rate = self.alpha * rate + (1 - self.alpha) * self.rate
			logging.debug("Average rate after: %s", self.rate)

		#cache the current buffer index
		index = self.buffer_state

		if self.buffer_state == (len(self.buffer) - 1):
		#at the end
			self.buffer = []
			self.buffer_state = 0
		else:
		#at the middle
			self.buffer_state += 1

		return self.output_dict[index]

			






#this is a test class to test to see 
#if buffermap is working well
class TestBufferMap(BufferMap):

	"""map() must be implemented by each subclass.
	"""
	def map(self, data):
		print('[Buffer Map] Processed: ',len(data), 'frames')
		return [{}]*len(data)




