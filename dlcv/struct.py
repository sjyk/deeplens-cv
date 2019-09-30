"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

struct.py defines the main data structures used in dlcv. 
"""
import cv2
import numpy as np

#sources video from the default camera
DEFAULT_CAMERA = 0


class VideoStream():
	"""The video stream class opens a stream of video
	   from a source.
	"""

	def __init__(self, src, limit=-1):
		"""Constructs a videostream object

		   Input: src- Source camera or file or url
		          limit- Number of frames to pull
		"""
		self.src = src
		self.limit = limit


	def __getitem__(self, xform):
		"""Applies a transformation to the video stream
		"""
		return xform.apply(self)


	def __iter__(self):
		"""Constructs the iterator object and initializes
		   the iteration state
		"""

		self.cap = cv2.VideoCapture(self.src)

		if not self.cap.isOpened():
			raise CorruptedOrMissingVideo(str(self.src) + " is corrupted or missing.")


		#set sizes after the video is opened
		self.width = int(self.cap.get(3))   # float
		self.height = int(self.cap.get(4)) # float

		self.frame_count = 0

		return self


	def __next__(self):
		if self.cap.isOpened() and \
		   (self.limit < 0 or self.frame_count <= self.limit):

		   	ret, frame = self.cap.read()

		   	if ret:
		   		self.frame_count += 1
		   		return {'data': frame, 'frame': (self.frame_count - 1)}

		   	else:
		   		raise StopIteration("Iterator is closed")

			
		else:
			raise StopIteration("Iterator is closed")


#helper methods
def getFrameData(frame):
	return frame['data']

def getFrameNumber(frame):
	return frame['frame']

def getFrameMetaData(frame):
	return {k:frame[k] for k in frame if k != 'data'}



class CorruptedOrMissingVideo(Exception):
   """Raised when opencv cannot open a video"""
   pass

class VideoNotFound(Exception):
   """Video with the specified name not found in the manager"""
   pass

class ManagerIOError(Exception):
   """Unspecified error with the manager"""
   pass



class Operator():
	"""This defines the api for the operator class
	"""

	def super_iter(self):
		self.width = self.video_stream.width
		self.height = self.video_stream.height

	def __getitem__(self, xform):
		"""Applies a transformation to the video stream
		"""
		return xform.apply(self)

	def apply(self, vstream):
		self.video_stream = vstream
		return self


	def logical_plan(self):
		if isinstance(self.video_stream, VideoStream):
			return [self.video_stream, self]
		else:
			return self.video_stream.logical_plan() + [self]



class Box():

	def __init__(self,x0,y0,x1,y1):
		self.x0 = x0
		self.y0 = y0
		self.x1 = x1
		self.y1 = y1

	def area(self):
		return np.abs((x1-x0)*(y1-y0))

	def _zero_x_cond(self, other):
		return (other.x0 >= self.x0 and other.x0 <= self.x1)

	def _zero_y_cond(self, other):
		return (other.y0 >= self.y0 and other.y0 <= self.y1)

	def _one_x_cond(self, other):
		return (other.x1 >= self.x0 and other.x1 <= self.x1)

	def _one_y_cond(self, other):
		return (other.y1 >= self.y0 and other.y1 <= self.y1)

	def contains(self, other):
		return self._zero_x_cond(other) and \
			   self._zero_y_cond(other) and \
			   self._one_x_cond(other) and \
			   self._one_y_cond(other)

	def intersect(self, other):
		return self._zero_x_cond(other) or \
			   self._zero_y_cond(other) or \
			   self._one_x_cond(other) or \
			   self._one_y_cond(other)

	def serialize(self):
		return int(self.x0),int(self.x1),int(self.y0),int(self.y1)


def build(plan):
	if len(plan) == 0:
		raise ValueError("Plan is empty")
	elif len(plan) == 1:
		return plan[0]
	else:
		v = plan[0]
		for op in plan[1:]:
			v = v[op]
		return v

