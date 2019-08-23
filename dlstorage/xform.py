"""This module describes the basic video transformations
"""

class VideoTransform(): 
	"""Applies a transformation to the stream of frames returning 
	a new stream
	"""

	def __init__(self):
		pass

	def apply(self, vstream):
		self.vstream = vstream
		return self

	def __iter__(self):
		self.input_iter = iter(self.vstream)
		return self

	def __next__(self):
		raise NotImplemented("Abstract wrapper: __next__() not implemented")



class Sample(VideoTransform): 
	"""Implements the sampling transformation
	"""

	def __init__(self, ratio):
		self.skip = int(1.0/ratio)
		super(Sample, self).__init__()

	def __next__(self):
		out = next(self.input_iter)
		if (self.vstream.frame_count-1) % self.skip == 0:
			return out
		else:
			return self.__next__()

