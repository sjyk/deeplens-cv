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


class DataStream():
	def __init__(self, file):
		raise NotImplementedError("initialize not implemented")

	def __next__(self):
		raise NotImplementedError("next not implemented")


class JSONListDataStream(DataStream):
	def __init__(self, files):
		self.data = []
		for file in files:
			with open(file, 'r') as f:
				self.data.append(iter(json.load(f)))

	def __next__(self):
		frame = []
		for d in self.data:
			frame.append(next(d))
		return frame


class ConstantDataStream(DataStream):
	def __init__(self, constants):
		self.data = constants

	def __next__(self):
		return self.data


class VideoStream():
	def __init__(self, src, limit=-1, origin=np.array((0, 0))):
		self.limit = limit
		self.src = src
		self.limit = limit
		self.origin = origin

	def lineage(self):
		return [self]


class CVVideoStream():
	"""The video stream class opens a stream of video
	   from a source.

	Frames are structured in the following way: (1) each frame
	is a dictionary where frame['data'] is a numpy array representing
	the image content, (2) all the other keys represent derived data.

	All geometric information (detections, countours) go into a list called
	frame['bounding_boxes'] each element of the list is structured as:
	(label, box).
	"""

	def __init__(self, src, limit=-1, origin=np.array((0,0)), offset=0, rows=[], hwang=False):
		"""Constructs a videostream object

		   Input: src- Source camera or file or url
				  limit- Number of frames to pull
				  origin- Set coordinate origin
		"""
		super().__init__(src, limit, origin)
		self.propIds = None
		self.cap = None
		self.time_elapsed = 0

		# moved from __iter__ to __init__ due to continuous iterating
		import cv2
		self.offset = offset
		self.frame_count = offset
		self.cap = cv2.VideoCapture(self.src)

	def __getitem__(self, xform):
		"""Applies a transformation to the video stream
		"""
		return xform.apply(self)

	def __iter__(self):
		"""Constructs the iterator object and initializes
		   the iteration state
		"""
		if self.cap == None:
			# iterate the same videostream again after the previous run has finished
			self.frame_count = self.offset
			self.cap = cv2.VideoCapture(self.src)

		if self.propIds:
			for propId in self.propIds:
				self.cap.set(propId, self.propIds[propId])

		if not self.cap.isOpened():
			raise CorruptedOrMissingVideo(str(self.src) + " is corrupted or missing.")

		#set sizes after the video is opened
		self.width = int(self.cap.get(3))   # float
		self.height = int(self.cap.get(4)) # float

		return self

	def __next__(self):
		if self.cap.isOpened() and \
		   (self.limit < 0 or self.frame_count < self.limit):

			time_start = timer()
			ret, frame = self.cap.read()
			self.time_elapsed += timer() - time_start

			if ret:
				self.frame_count += 1
				return {'data': frame, \
						'frame': (self.frame_count - 1), \
						'origin': self.origin,
						'width': self.width,
						'height': self.height}
			else:
				raise StopIteration("Iterator is closed")
		else:
			# self.cap.release()  # commented out due to CorruptedOrMissingVideo error
			self.cap = None
			raise StopIteration("Iterator is closed")

	def __call__(self, propIds = None):
		""" Sets the propId argument so that we can
		take advantage of video manipulation already
		supported by VideoCapture (cv2)
		Arguments:
			propIds: {'ID': property}
		"""
		self.propIds = propIds

	def get_cap_info(self, propId):
		""" If we currently have a VideoCapture op
		"""
		if self.cap:
			return self.cap.get(propId)
		else:
			return None

	def lineage(self):
		return [self]


class IteratorVideoStream(VideoStream):
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
		self.global_lineage = []

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

		try:
			self.next_frame = next(self.frame_iter)
			# set sizes after the video is opened
			if 'data' in self.next_frame:
				self.width = int(self.next_frame['data'].shape[0])  # float
				self.height = int(self.next_frame['data'].shape[1])  # float

			self.frame_count = 1
		except StopIteration:
			self.next_frame = None

		return self

	def __next__(self):
		if self.next_frame == None:
			raise StopIteration("Iterator is closed")

		if (self.limit < 0 or self.frame_count <= self.limit):
			ret = self.next_frame
			self.next_frame = next(self.frame_iter)

			if 'frame' in ret:
				return ret
			else:
				self.frame_count += 1
				ret.update({'frame': (self.frame_count - 1)})
				return ret
		else:
			raise StopIteration("Iterator is closed")

	def lineage(self):
		return self.global_lineage


class RawVideoStream(VideoStream):
	"""The video stream class opens a stream of video
	   from an iterator over frames (e.g., a sequence
	   of png files). Compatible with opencv streams.
	"""

	def __init__(self, src, limit=-1, origin=np.array((0,0))):
		"""Constructs a videostream object

		   Input: src- iterator over frames
				  limit- Number of frames to pull
		"""
		self.src = src
		self.limit = limit
		self.global_lineage = []
		self.origin = origin

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

		try:
			self.next_frame = next(self.frame_iter)
			# set sizes after the video is opened
			self.width = int(self.next_frame.shape[0])  # float
			self.height = int(self.next_frame.shape[1])  # float

			self.frame_count = 1
		except StopIteration:
			self.next_frame = None

		return self

	def __next__(self):
		if self.next_frame is None:
			raise StopIteration("Iterator is closed")

		if (self.limit < 0 or self.frame_count <= self.limit):
			ret = self.next_frame
			self.next_frame = next(self.frame_iter)
			self.frame_count += 1
			return {'frame': (self.frame_count - 1),
					'data': ret,
					'origin': self.origin,
					'width': self.width,
					'height': self.height}
		else:
			raise StopIteration("Iterator is closed")

	def lineage(self):
		return self.global_lineage


class HwangVideoStream(VideoStream):
	def __init__(self, src, limit=-1, origin=np.array((0, 0)), rows=[]):
		super().__init__(src, limit, origin)
		import hwang
		self.rows = rows
		self.frame_count = 0
		self.decoder = hwang.Decoder(self.src)

	def __iter__(self):
		self.width = self.decoder.video_index.frame_width()
		self.height = self.decoder.video_index.frame_height()

		# TODO(swjz): fetch all rows when (limit == -1)
		self.frames = iter(self.decoder.retrieve(self.rows))

	def __next__(self):
		self.frame_count += 1
		return {'data': next(self.frames),
				'frame': (self.frame_count - 1),
				'origin': self.origin}


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


class DataStreamOperator():
	"""An operator defines consumes an iterator over DataStream
	and produces and iterator over DataStream. The Operator class
	is the abstract class of all pipeline components in DeepLens.

	We overload python subscripting to construct a pipeline
	>> stream[Transform()]
	"""

	#subscripting binds a transformation to the current stream
	def apply(self, data_stream):
		self.data_stream = data_stream
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
		if isinstance(self.data_stream, DataStream):
			return [self.data_stream, self]
		else:
			return self.data_stream.lineage() + [self]

	def _serialize(self):
		return NotImplementedError("This operator cannot be serialized")

	def serialize(self):
		try:
			import json
			return json.dumps(self._serialize())
		except:
			return ManagerIOError("Serialization Error")


class VideoStreamOperator():
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
		return NotImplementedError("This operator cannot be serialized")

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


class CustomTagger(VideoStreamOperator):
	def __init__(self, tagger, batch_size):
		super(CustomTagger, self).__init__()
		# a custom tagger function that takes video_stream and batch_size; it raises StopIteration when finishes
		self.tagger = tagger
		self.batch_size = batch_size
		self.next_count = 0  # how many next() we have called after _get_tags()
		self.stream = False
		self.frames = []

	def __iter__(self):
		self.input_iter = iter(self.video_stream)
		self.super_iter()
		return self

	def _get_tags(self):
		if self.next_count == 0 or self.next_count >= self.batch_size:
			self.next_count = 0
			# we assume it iterates the entire batch size and save the results
			self.tags = []
			try:
				if self.stream:
					tag, frames = self.tagger(self.input_iter, self.batch_size, video = True)
					self.frames = frames
				else:
					tag = self.tagger(self.input_iter, self.batch_size)
			except StopIteration:
				raise StopIteration("Iterator is closed")
			if tag:
				self.tags.append(tag)

		self.next_count += 1
		return self.tags

	def __next__(self):
		if self.stream:
			tags = self._get_tags()
			return {'objects': tags, 'frame': self.frames[self.next_count - 1]}
		else:
			return {'objects': self._get_tags()}

	def set_stream(self, stream):
		self.stream = stream

class Serializer(json.JSONEncoder):
	def default(self, obj):
		return obj.serialize()
