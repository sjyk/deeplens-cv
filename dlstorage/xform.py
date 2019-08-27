"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

xform.py defines transformations to video streams. These transformations
act as dataflow operators that produce and consume iterators over frames.
"""

import random
import string


"""Video data first starts off as a VideoStream and transformations convert 
into into a VideoTransform object. Like VideoStreams these objects are also 
iterators over frames
"""
class VideoTransform(): 
	"""The VideoTransform class is the basic abstract class for all video 
	transformations. All derived classes must implement a __next__() method.
	"""

	#dummy constructor that doesn't do anything now
	def __init__(self):
		pass

	def apply(self, vstream):
		"""apply() initializes the object with a particular input video 
		stream.

		Args:
			vstream (Iterable of Frames)
		"""
		self.vstream = vstream
		return self


	def __iter__(self):
		"""__iter__() initializes the iterator. We use this steps to cache 
		height and width data.
		"""
		self.input_iter = iter(self.vstream)
		self.width = self.vstream.width
		self.height = self.vstream.height
		return self


	#Must be implemented by any derived classes
	def __next__(self):
		raise NotImplemented("All VideoTransform classes must implement __next__()")


class Sample(VideoTransform): 
	"""Sample is a transform that drops frames from a video stream at a given 
	rate. 
	"""

	def __init__(self, ratio):
		"""Sample is constructed with a sampling ratio of 0-1, which denotes 
		the frequency of frames to skip.

		Args:
			ratio (float) - the frequency of frames to skip
		"""
		self.skip = int(1.0/ratio)
		super(Sample, self).__init__()


	def __next__(self):
		"""This implements the skipping logic for the Sampling transformation
		"""
		out = next(self.input_iter)
		if (self.vstream.frame_count-1) % self.skip == 0:
			return out
		else:
			return self.__next__()

