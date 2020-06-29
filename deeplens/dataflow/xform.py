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
						break

			self.prev = ff['bounding_boxes']

			ff['bounding_boxes'] = rtn

			return ff


class MotionVectors(Map):
	#removes transient keypoints

	def __init__(self,distance=10):
		self.distance = distance

	def __iter__(self):
		self.prev = None
		return super().__iter__()


	def map(self, data):

		ff = data

		feature_params = dict( maxCorners = 100,
                               qualityLevel = 0.3,
                               minDistance = 3,
                               blockSize = 3 )

		lk_params = dict( winSize  = (15,15),
                          maxLevel = 3,
                          criteria = (cv2.TERM_CRITERIA_EPS | cv2.TERM_CRITERIA_COUNT, 10, 0.03))


		if self.prev is None or self.prev.shape != ff['data'].shape:
			self.prev = ff['data']
			ff['motion_vectors'] = []
			return ff
		else:		
			old_gray = cv2.cvtColor(self.prev, cv2.COLOR_BGR2GRAY)
			frame_gray = cv2.cvtColor(ff['data'], cv2.COLOR_BGR2GRAY)
			p0 = cv2.goodFeaturesToTrack(old_gray, mask = None, **feature_params)

			if p0 is None:
				ff['motion_vectors'] = []
				return ff

			p1, st, err = cv2.calcOpticalFlowPyrLK(old_gray, frame_gray, p0, None, **lk_params)

			p0p1 = np.hstack((np.squeeze(p0), np.squeeze(p1))).reshape(-1, 4)

			ff['motion_vectors'] = []
			for i in range(p0p1.shape[0]):
				dist = np.sqrt((p0p1[i,0] - p0p1[i,2])**2 + (p0p1[i,1] - p0p1[i,3])**2)
				if dist > 0.1:
					ff['motion_vectors'].append(tuple(p0p1[i,:]))

			self.prev = ff['data']
			return ff


class Direction(Map):

	def quadrant(self, vec):
		dx = vec[2]-vec[0]
		dy = vec[3]-vec[1]
		distance = np.sqrt(dx**2 + dy**2)

		if distance == 0:
			return 0

		return np.arccos(dx/distance)
		

	def map(self, data):
		ff = data
		ff['motion_vectors'] = list(map(self.quadrant, ff['motion_vectors']))
		return ff

class Speed(Map):

	def quadrant(self, vec):
		dx = vec[2]-vec[0]
		dy = vec[3]-vec[1]
		distance = np.sqrt(dx**2 + dy**2)

		return distance
		

	def map(self, data):
		ff = data
		ff['motion_vectors'] = list(map(self.quadrant, ff['motion_vectors']))
		return ff



class Null(Map):

	def map(self, data):
		return data

