"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

event.py defines some of the main detection primitives used in dlcv.
"""

from deeplens.struct import Operator, Box
from deeplens.dataflow.map import Map
import numpy as np
from timeit import default_timer as timer

"""in dlcv there are Metrics and Events. Metrics translate geometric
vision primtives into numerical time-series, and Events detect patterns
in these time-series.
"""

class Metric(Map):

	def __init__(self, name, region):
		"""A generic metric class takes in a name and a region of interest.
		The output of this metric is stored in frame[name].
		"""
		self.name = name
		self.region = region


	def __getitem__(self, xform):
		"""Applies a transformation to the video stream
		"""
		res = xform.apply(self)

		return res


class ActivityMetric(Metric):
	"""The most basic metric is the activity metric
	which identifies if a certain object has a bounding
	box contained in a region.
	"""

	def __init__(self, name, region, filter="object"):
		self.filter = filter
		super(ActivityMetric, self).__init__(name, region)
		

	def map(self, data):
		cnt = 0

		#print(self.region)

		for label, pt in data['bounding_boxes']:
			box = Box(*pt)

			if label == self.filter and \
				self.region.shift(data['origin']).contains(box):
				cnt += 1

		data[self.name] = cnt
		#print(cnt)
		return data

	def _serialize(self):
		return {'name': self.name,
				'region': ('Box', {'x0': self.region.x0, \
					               'x1': self.region.x1,
					               'y0': self.region.y0,
					               'y1': self.region.y1
								  }),
				'filter': self.filter}



class Filter(Operator):
	"""Filter() defines cross-correlation kernel and a threshold. It
	slides this kernel across the metric and if this threshold is exceeded
	it defines an event {True, False} variable.
	"""

	def __init__(self, name, kernel, threshold, delay=0):
		"""Name is the metric, kernel is a list of numbers defining a 
		cross-correlation kernel, threshold is a threshold on the value,
		and the delay is the minimum time between events.
		"""
		self.kernel = kernel
		self.threshold = threshold
		self.name = name
		self.delay = delay
		self.time_elapsed = 0


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
			#print('T',np.array(self.kernel).dot(np.array(self.buffer)))
			return True
		else:
			#print('F',np.array(self.kernel).dot(np.array(self.buffer)))
			return False

	def __next__(self):
		out = next(self.frame_iter)

		time_start = timer()
		self.buffer.append(out[self.name])
		self.buffer = self.buffer[1:len(self.kernel)+1]

		#print(self.last_event + self.delay)

		if self.frame_count > self.last_event + self.delay:
			if self._dot():
				#print('frame hit',out['frame'])
				out[self.name] = True
				self.last_event = self.frame_count
			else:
				out[self.name] = False
		else:
			out[self.name] = False

		self.frame_count += 1
		self.time_elapsed += timer() - time_start

		return out

	def _serialize(self):
		return {'kernel': self.kernel,
				'threshold': self.threshold,
		        'name':self.name,
		         'delay': self.delay}


##applies classifier to region if true
class ActivityClassifier(Map):

	def __init__(self, trigger, name, region, delay=0):
		self.trigger = trigger
		self.name = name
		self.region = region
		self.last_event = None
		self.delay = delay

		super(ActivityClassifier, self).__init__()

	def classify(self, img):
		raise ValueError('Not Implemented')

	def map(self, data):
		if data[self.trigger]:
			self.last_event = data['frame']

		#not (self.last_event is None) and (data['frame'] == self.last_event + self.delay)
		if not (self.last_event is None) and (data['frame'] == self.last_event + self.delay):
			ff = data

			region_s = self.region.shift(data['origin'])
			x0, y0, x1, y1 = region_s.x0,region_s.y0,region_s.x1,region_s.y1

			cropped_image = ff['data'][y0:y1,x0:x1]
			ff[self.name] = self.classify(cropped_image)
			return ff
		else:
			ff = data
			ff[self.name] = False
			return ff



