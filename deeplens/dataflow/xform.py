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
		ff['data'] = cv2.cvtColor(ff['data'],cv2.COLOR_GRAY2RGB)
		return ff


class Canny(Map):

	def __init__(self,edge_low=225, edge_high=250):
		self.edge_low = edge_low
		self.edge_high = edge_high

	def map(self, data):
		ff = data
		ff['data'] = cv2.Canny(ff['data'], self.edge_low, self.edge_high)
		return ff


class RunningBackground(Map):

	def __iter__(self):
		self.prev = None
		return super().__iter__()

	def map(self, data):

		ff = data

		if self.prev is None:
			self.prev = ff['data']
			return ff
		else:		
			mask = np.abs((self.prev - ff['data']) > 0).astype(np.uint8)

			self.prev = ff['data']

			img = ff['data']*mask
			#kernel = np.ones((self.filter,self.filter),np.uint8)

			ff['data'] = cv2.bilateralFilter(img,9,150,150)

			return ff


class KeyPointFilter(Map):
	#removes transient keypoints

	def __init__(self,distance=5):
		self.distance = distance

	def __iter__(self):
		self.prev = None
		return super().__iter__()


	def map(self, data):

		ff = data

		if self.prev is None:
			self.prev = ff['bounding_boxes']
			return ff
		else:		
			
			rtn = []
			for label, bbi in ff['bounding_boxes']:
				for _, bbj in self.prev:
					dist = np.sqrt((bbi[0]-bbj[0])**2 + (bbi[1]-bbj[1])**2)

					if dist < self.distance: 
						rtn.append((label, bbi))

			self.prev = ff['bounding_boxes']

			ff['bounding_boxes'] = rtn

			return ff




class Null(Map):

	def map(self, data):
		return data

