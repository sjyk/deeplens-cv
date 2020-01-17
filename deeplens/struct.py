"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

struct.py defines the main data structures used in deeplens. It defines a
video input stream as well as operators that can transform this stream.
"""
import cv2
from deeplens.error import *
import numpy as np

#sources video from the default camera
DEFAULT_CAMERA = 0


class VideoStream():
	"""The video stream class opens a stream of video
	   from a source.

	Frames are structured in the following way: (1) each frame 
	is a dictionary where frame['data'] is a numpy array representing
	the image content, (2) all the other keys represent derived data.

	All geometric information (detections, countours) go into a list called
	frame['bounding_boxes'] each element of the list is structured as:
	(label, box).
	"""

	def __init__(self, src, limit=-1, origin=np.array((0,0))):
		"""Constructs a videostream object

		   Input: src- Source camera or file or url
		          limit- Number of frames to pull
		          origin- Set coordinate origin
		"""
		self.src = src
		self.limit = limit
		self.origin = origin


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
		   (self.limit < 0 or self.frame_count < self.limit):

		   	ret, frame = self.cap.read()

		   	if ret:
		   		self.frame_count += 1
		   		return {'data': frame, \
		   				'frame': (self.frame_count - 1),\
		   				'origin': self.origin}

		   	else:
		   		raise StopIteration("Iterator is closed")

			
		else:
			raise StopIteration("Iterator is closed")


class IteratorVideoStream():
	"""The video stream class opens a stream of video
	   from an iterator over frames (e.g., a sequence
	   of png files). Compatible with opencv streams.
	"""

	def __init__(self, src, limit=-1):
		"""Constructs a videostream object

		   Input: src- iterator over frames
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

		try:
			self.frame_iter = iter(self.src)
		except:
			raise CorruptedOrMissingVideo(str(self.src) + " is corrupted or missing.")

		self.next_frame = next(self.frame_iter)

		# set sizes after the video is opened
		self.width = int(self.next_frame.shape[0])  # float
		self.height = int(self.next_frame.shape[1])  # float

		self.frame_count = 1

		return self

	def __next__(self):
		if (self.limit < 0 or self.frame_count <= self.limit):

			ret = self.next_frame
			self.next_frame = next(self.frame_iter)
			self.frame_count += 1

			return {'data': ret, 'frame': (self.frame_count - 1)}
		else:
			raise StopIteration("Iterator is closed")


#helper methods
def getFrameData(frame):
	return frame['data']

def getFrameNumber(frame):
	return frame['frame']

def getFrameMetaData(frame):
	return {k:frame[k] for k in frame if k != 'data'}

#given a list of pipeline methods, it reconstucts it into a stream
def build(lineage):
	"""build(lineage) takes as input the lineage of a stream and
	constructs the stream.
	"""
	plan = lineage
	if len(plan) == 0:
		raise ValueError("Plan is empty")
	elif len(plan) == 1:
		return plan[0]
	else:
		v = plan[0]
		for op in plan[1:]:
			v = v[op]
		return v


class Operator():
	"""An operator defines consumes an iterator over frames
	and produces and iterator over frames. The Operator class
	is the abstract class of all pipeline components in dlcv.
	
	We overload python subscripting to construct a pipeline
	>> stream[Transform()] 
	"""

	#this is a function that sets some bookkeepping variables
	#that are useful for sizing streams without opening the frames.
	def super_iter(self):
		self.width = self.video_stream.width
		self.height = self.video_stream.height
		self.frame_count = self.video_stream.frame_count

	def super_next(self):
		self.frame_count = self.video_stream.frame_count

	#subscripting binds a transformation to the current stream
	def apply(self, vstream):
		self.video_stream = vstream
		return self

	def __getitem__(self, xform):
		"""Applies a transformation to the video stream
		"""
		return xform.apply(self)

	def lineage(self):
		"""lineage() returns the sequence of transformations
		that produces the given stream of data. It can be run
		without materializing any of the stream.

		Output: List of references to the pipeline components
		"""
		if isinstance(self.video_stream, VideoStream):
			return [self.video_stream, self]
		else:
			return self.video_stream.lineage() + [self]

	def _serialize(self):
		return NotImplemented("This operator cannot be serialized")

	def serialize(self):
		try:
			import json
			return json.dumps(self._serialize())
		except:
			return ManagerIOError("Serialization Error")




class Box():
	"""Bounding boxes are a core geometric construct in the dlcv
	system. The Box() class defines a bounding box with named coordinates
	and manipulation methods to determine intersection and containment.
	"""

	def __init__(self,x0,y0,x1,y1):
		"""The constructor for a box, all of the inputs have to be castable to 
		integers. By convention x0 <= x1 and y0 <= y1
		"""
		self.x0 = x0
		self.y0 = y0
		self.x1 = x1
		self.y1 = y1

		if x0 > x1 or y0 > y1:
			raise InvalidRegionError("The specified box is invalid: " + str([x0,y0,x1,y1]))

	#shifts the box to a new origin
	def shift(self, origin):
		return Box(self.x0-origin[0], \
				   self.y0-origin[1], \
				   self.x1-origin[0], \
				   self.y1-origin[1])

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
		return self._zero_x_cond(other) or \
			   self._zero_y_cond(other) or \
			   self._one_x_cond(other) or \
			   self._one_y_cond(other)

	"""The storage manager needs a tuple representation of the box, this serializes it.
	"""
	def serialize(self):
		return int(self.x0),int(self.y0),int(self.x1),int(self.y1)


