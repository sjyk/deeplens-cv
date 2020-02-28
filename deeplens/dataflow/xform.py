import cv2
from deeplens.dataflow.map import Map

class Grayscale(Map):

	def map(self, data):
		ff = data
		ff['data'] = cv2.cvtColor(ff['data'], cv2.COLOR_BGR2GRAY)
		return ff


class Blur(Map):

	def __init__(self,filter):
		self.filter = filter

	def map(self, data):
		ff = data
		ff['data'] = cv2.GaussianBlur(ff['data'], (self.filter, self.filter), 0)
		return ff

