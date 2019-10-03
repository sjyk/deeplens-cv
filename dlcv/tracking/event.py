
from dlcv.struct import Operator, Box
from dlcv.dataflow.map import Map
import numpy as np

class Metric(Map):

	def __init__(self, name, region):
		self.name = name
		self.region = region


	def __getitem__(self, xform):
		"""Applies a transformation to the video stream
		"""
		res = xform.apply(self)

		return res


class ActivityMetric(Metric):

	def __init__(self, name, region, filter="object"):
		self.filter = filter
		super(ActivityMetric, self).__init__(name, region)
		

	def map(self, data):
		cnt = 0

		for label, pt in data['bounding_boxes']:
			box = Box(*pt)

			if label == self.filter and \
				self.region.contains(box):
				cnt += 1

		data[self.name] = cnt
		return data


class Filter(Operator):

	def __init__(self, name, kernel, threshold, delay=0):
		self.kernel = kernel
		self.threshold = threshold
		self.name = name
		self.delay = delay

	def __iter__(self):
		self.frame_iter = iter(self.video_stream)
		self.super_iter()
		self.buffer = [None] * len(self.kernel)
		self.frame_count = 0
		self.last_event = -self.delay + 1
		return self

	def _dot(self):
		if None in self.buffer:
			return False
		elif np.array(self.kernel).dot(np.array(self.buffer)) > self.threshold:
			return True
		else:
			return False

	def __next__(self):
		"""This implements the skipping logic for the Sampling transformation
		"""
		out = next(self.frame_iter)
		
		self.buffer.append(out[self.name])
		self.buffer = self.buffer[1:len(self.kernel)+1]

		if self.frame_count > self.last_event + self.delay:
			if self._dot():
				out[self.name] = True
				self.last_event = self.frame_count
			else:
				out[self.name] = False
		else:
			out[self.name] = False

		self.frame_count += 1

		return out

