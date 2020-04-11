
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

	def __init__(self, src, limit=-1, origin=np.array((0,0)), offset=0, rows=[], hwang=False):
		"""Constructs a videostream object

		   Input: src- Source camera or file or url
				  limit- Number of frames to pull
				  origin- Set coordinate origin
		"""
		self.src = src
		self.limit = limit
		self.origin = origin
		self.propIds = None
		self.cap = None
		self.time_elapsed = 0
		self.hwang = hwang

		# moved from __iter__ to __init__ due to continuous iterating
		if not hwang:
			self.offset = offset
			self.frame_count = offset
			self.cap = cv2.VideoCapture(self.src)
		else:
			import hwang
			self.rows = rows
			self.frame_count = 0
			self.decoder = hwang.Decoder(self.src)

	def __getitem__(self, xform):
		"""Applies a transformation to the video stream
		"""
		return xform.apply(self)


	def __iter__(self):
		"""Constructs the iterator object and initializes
		   the iteration state
		"""
		if not self.hwang:
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
		else:
			self.width = self.decoder.video_index.frame_width()
			self.height = self.decoder.video_index.frame_height()
			self.frames = iter(self.decoder.retrieve(self.rows))

		return self


	def __next__(self):
		if not self.hwang:
			if self.cap.isOpened() and \
			   (self.limit < 0 or self.frame_count < self.limit):

				time_start = timer()
				ret, frame = self.cap.read()
				self.time_elapsed += timer() - time_start

				if ret:
					self.frame_count += 1
					return {'data': frame, \
							'frame': (self.frame_count - 1),\
							'origin': self.origin}
				else:
					raise StopIteration("Iterator is closed")
			else:
				# self.cap.release()  # commented out due to CorruptedOrMissingVideo error
				self.cap = None
				raise StopIteration("Iterator is closed")
		else:
			self.frame_count += 1
			return {'data': next(self.frames),
					'frame': (self.frame_count - 1),
					'origin': self.origin}

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
			return {'frame': (self.frame_count - 1), 'data': ret, 'origin': self.origin}
		else:
			raise StopIteration("Iterator is closed")

	def lineage(self):
		return self.global_lineage


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


