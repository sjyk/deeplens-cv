"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

contour.py defines geometric vision primitives.
"""

from deeplens.dataflow.map import Map
from timeit import default_timer as timer
import cv2
import numpy as np

class KeyPoints(Map):
	"""KeyPoints uses a canny edge detector for identifying any object
	 (but not particular classes). You can tag these detections with a 
	 generic label "unknown" or "object" or whatever you want.
	"""

	def __init__(self, \
				 blur=5, \
				 edge_low=225, \
				 edge_high=250, \
				 area_thresh=10,
				 label="object"):
		"""The constructor takes in some parameters for the detector.
		"""

		self.blur = blur
		self.edge_low = edge_low
		self.edge_high = edge_high
		self.area_thresh = area_thresh
		self.label = label
		self.scale = 1.0

	"""the map function creates bounding boxes of the form x,y,x,y to identify detection points
	"""
	def map(self, data):
		ff = data

		#print(ff['data'].shape)

		#now = timer()

		if len(ff['data'].shape) < 3:
			gray = ff['data']
		else:
			gray = ff['data'][:,:,0]


		#print('Copy Elapsed: ', timer() - now)

		blurred = cv2.GaussianBlur(gray, (self.blur, self.blur), 0)
		tight = cv2.Canny(blurred, self.edge_low, self.edge_high)

		contours, _ = cv2.findContours(tight.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
		
		rtn = []
		for cnt in contours:
			if cv2.contourArea(cnt) > self.area_thresh:
				M = cv2.moments(cnt)
				cx = int(M['m10']/M['m00'])
				cy = int(M['m01']/M['m00'])

				rtn.append((self.label,(cx,cy,cx,cy)))

		#print(len(rtn))
		ff['bounding_boxes'] = rtn

		return ff


	def _serialize(self):
		return {'blur': self.blur,
				'edge_low': self.edge_low,
				'edge_high': self.edge_high,
				'area_thresh': self.area_thresh,
				'label': self.label}

class SizeMovementDetector(KeyPoints):
	"""Categorizes Moving Objects By Size
	"""

	def __init__(self, \
				 blur=5, \
				 bilat=150,
				 edge_low=40, \
				 edge_high=50, \
				 area_thresh=500): #min size
		"""The constructor takes in some parameters for the detector.
		"""

		self.blur = blur
		self.edge_low = edge_low
		self.edge_high = edge_high
		self.area_thresh = area_thresh
		self.bilat = bilat
		self.scale = 1.0

	def __iter__(self):
		self.prev = None
		return super().__iter__()

	"""the map function creates bounding boxes of the form x,y,x,y to identify detection points
	"""
	def map(self, data):
		ff = data

		if len(ff['data'].shape) < 3:
			gray = ff['data']
		else:
			gray = ff['data'][:,:,0]


		blurred = cv2.GaussianBlur(gray, (self.blur, self.blur), 0)
		tight = cv2.Canny(blurred, self.edge_low, self.edge_high)

		if not (self.prev is None):
			mask = np.abs((self.prev - tight) > 0).astype(np.uint8)
			self.prev = tight

			img = tight*mask
			tight = cv2.bilateralFilter(img,7,self.bilat,self.bilat)
		else:
			self.prev = tight


		contours, _ = cv2.findContours(tight.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
		
		rtn = []
		for cnt in contours:
			if cv2.contourArea(cnt) > self.area_thresh:
				M = cv2.moments(cnt)
				cx = int(M['m10']/M['m00'])
				cy = int(M['m01']/M['m00'])

				rtn.append(('object',(cx,cy,cx,cy)))

		#print(len(rtn))
		ff['bounding_boxes'] = rtn

		return ff


	def _serialize(self):
		return {'blur': self.blur,
				'edge_low': self.edge_low,
				'edge_high': self.edge_high,
				'area_thresh': self.area_thresh,
				'label': self.label}


class GoodKeyPoints(KeyPoints):

	def __init__(self, \
				 maxCorners = 1500,\
                 qualityLevel = 0.2,\
                 minDistance = 25,\
                 blockSize = 9,\
                 blur=1):

		self.maxCorners = maxCorners
		self.qualityLevel = qualityLevel
		self.minDistance = minDistance
		self.blockSize = blockSize
		self.blur = blur
		self.area_thresh = 1

	def map(self, data):

		ff = data

		if len(ff['data'].shape) < 3:
			gray = ff['data']
		else:
			gray = ff['data'][:,:,0]


		feature_params = dict( maxCorners = self.maxCorners,
                               qualityLevel = self.qualityLevel,
                               minDistance = self.minDistance,
                               blockSize = self.blockSize )

		gray = cv2.cvtColor(ff['data'], cv2.COLOR_BGR2GRAY)
		p0 = cv2.goodFeaturesToTrack(gray, mask = None, **feature_params)

		bounding_boxes = []
		if p0 is not None:
			for i in p0:
				bounding_boxes.append(('object',(i[0,0], i[0,1], i[0,0],i[0,1])))

		ff['bounding_boxes'] = bounding_boxes

		return ff
