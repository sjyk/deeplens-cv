"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

struct.py defines the main data structures used in deeplens. It defines a
video input stream as well as operators that can transform this stream.
"""
from deeplens.error import *
import numpy as np
import json
from timeit import default_timer as timer

#sources video from the default camera
DEFAULT_CAMERA = 0

class Box():
	"""Bounding boxes are a core geometric construct in the dlcv
	system. The Box() class defines a bounding box with named coordinates
	and manipulation methods to determine intersection and containment.
	"""

	def __init__(self,x0,y0,x1,y1):
		"""The constructor for a box, all of the inputs have to be castable to
		integers. By convention x0 <= x1 and y0 <= y1
		"""
		self.x0 = int(x0)
		self.y0 = int(y0)
		self.x1 = int(x1)
		self.y1 = int(y1)

		if x0 > x1 or y0 > y1:
			raise InvalidRegionError("The specified box is invalid: " + str([x0,y0,x1,y1]))

	#shifts the box to a new origin
	def shift(self, origin):
		return Box(self.x0-origin[0], \
				   self.y0-origin[1], \
				   self.x1-origin[0], \
				   self.y1-origin[1])

	def x_translate(self, x):
		return Box(self.x0 + x, \
				   self.y0, \
				   self.x1 + x, \
				   self.y1)

	def y_translate(self, y):
		return Box(self.x0, \
				   self.y0 + y, \
				   self.x1, \
				   self.y1 + y)
	def area(self):
		"""Calculates the area contained in the box
		"""
		return np.abs((self.x1 - self.x0) * (self.y1 - self.y0))

	def __mul__(self, scalar):
		return Box(int(self.x0/scalar), \
				   int(self.y0/scalar), \
				   int(self.x1*scalar), \
				   int(self.y1*scalar))

	#helpher methods to test intersection and containement
	def _zero_x_cond(self, other):
		return (other.x0 >= self.x0 and other.x0 <= self.x1)

	def _zero_y_cond(self, other):
		return (other.y0 >= self.y0 and other.y0 <= self.y1)

	def _one_x_cond(self, other):
		return (other.x1 >= self.x0 and other.x1 <= self.x1)

	def _one_y_cond(self, other):
		return (other.y1 >= self.y0 and other.y1 <= self.y1)

	"""Intersection and containement
	"""
	def contains(self, other):
		return self._zero_x_cond(other) and \
			   self._zero_y_cond(other) and \
			   self._one_x_cond(other) and \
			   self._one_y_cond(other)

	def intersect(self, other):
		x = self.x1 >= other.x0 and other.x1 >= self.x0
		y = self.y1 >= other.y0 and other.y1 >= self.y0
		return x and y

	def intersect_area(self, other):
		if self.intersect(other):
			x = min(self.x1, other.x1) - max(self.x0, other.x0)
			if x < 0:
				x = 0
			y = min(self.y1, other.y1) - max(self.y0, other.y0)
			if y < 0:
				y = 0
			return x*y
		else:
			return 0

	def union_area(self, other):
		ia = self.intersect_area(other)
		return self.area() + other.area() - ia

	def union_box(self, other):
		return Box(min(self.x0, other.x0), \
				min(self.y0, other.y0), \
				max(self.x1, other.x1), \
				max(self.y1, other.y1))
	"""The storage manager needs a tuple representation of the box, this serializes it.
	"""
	def serialize(self):
		return int(self.x0),int(self.y0),int(self.x1),int(self.y1)