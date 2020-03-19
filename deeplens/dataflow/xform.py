import cv2
from deeplens.dataflow.map import Map
import numpy as np

class Grayscale(Map):

	def map(self, data):
		ff = data
		ff['data'] = cv2.cvtColor(ff['data'], cv2.COLOR_BGR2GRAY)
		return ff

class ColorQuant(Map):

	def __init__(self,depth):
		self.depth = int(depth)

	def map(self, data):
		ff = data

		ff['data'] = (np.floor(ff['data']*(self.depth/255))*(255/self.depth)).astype(np.uint8)

		return ff


class Blur(Map):

	def __init__(self,filter):
		self.filter = filter

	def map(self, data):
		ff = data
		ff['data'] = cv2.GaussianBlur(ff['data'], (self.filter, self.filter), 0)
		return ff

class Null(Map):

	def map(self, data):
		return data

