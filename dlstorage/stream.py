"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

This module describes a video iterator that returns an iterator over frames 
in a video
"""
import cv2

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
		   		return {'data': frame, 'frame': (self.frame_count - 1)}

		   	else:
		   		raise StopIteration("Iterator is closed")

			
		else:
			raise StopIteration("Iterator is closed")