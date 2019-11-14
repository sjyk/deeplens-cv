"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

buffer_map.py defines the main adaptive dataflow component in DeepLens. It 
leverages SIMD operations when possible. 
"""

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
				 buffer_size=1, \
				 sampling_rate=1, \
				 resolution=1):
		"""buffer_map takes in three parameters: 
		the size of the buffer, a sampling rate, and a
		resolution resizing.
		"""

		self.buffer_size = buffer_size
		self.skip = int(1.0/sampling_rate)
		self.resolution = resolution

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

		now = time.time()

		if self.resolution == 1:
			output = self.map([frame['data'] for frame in inp])
		else:
			output = self.map([cv2.resize(frame['data'],
										  (int(frame['data'].shape[1]*self.resolution),\
										   int(frame['data'].shape[0]*self.resolution))) \
									 for frame in inp])

		delta = time.time() - now

		self.output_dict = list(self.buffer)#copy
		self.rate = self.buffer_size/delta

		for i,_ in enumerate(self.output_dict):
			self.output_dict[i].update(output[i])

		for i,pred in enumerate(prediction_indices):
			self.output_dict[pred].update(output[i])

		#self.updateBatch()


	"""map() must be implemented by each subclass.
	"""
	def map(self, data):
		raise NotImplemented("BufferMap must implement a map function")



	#fill up the buffer
	def _fill_buffer(self):

		for i in range(self.buffer_size):
			frame = next(self.frame_iter, None)

			if frame == None:
				break

			self.buffer.append(frame)

	#iterator producer
	def __next__(self):

		if self.buffer_state == 0:
		#buffer is filled so run map first time
			self._fill_buffer()

			if len(self.buffer) == 0:
				raise StopIteration()

			self._map()

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




