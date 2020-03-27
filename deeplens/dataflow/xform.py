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

	def __init__(self,filter=5):
		self.filter = filter

	def map(self, data):
		ff = data
		ff['data'] = cv2.GaussianBlur(ff['data'], (self.filter, self.filter), 0)
		return ff

class Expand(Map):

	def map(self, data):
		ff = data
		ff['data'] = cv2.cvtColor(gray,cv2.COLOR_GRAY2RGB)
		return ff


class Canny(Map):

	def __init__(self,edge_low=225, edge_high=250):
		self.edge_low = edge_low
		self.edge_high = edge_high

	def map(self, data):
		ff = data
		ff['data'] = cv2.Canny(ff['data'], self.edge_low, self.edge_high)
		return ff


class Null(Map):

	def map(self, data):
		return data

