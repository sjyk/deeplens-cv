"""This module describes the basic video transformations
"""
import random
import string

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
		self.width = self.vstream.width
		self.height = self.vstream.height
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



class TestTagger(VideoTransform):
	def __init__(self):
		super(TestTagger, self).__init__()

	def _get_tags(self, img):
		tags = []
		label_set = {}
		for i in range(10):
			label = random.choice(string.ascii_lowercase)
			bb = (int(random.random()*self.vstream.width),
			      int(random.random()*self.vstream.height),
			      int(random.random()*self.vstream.width),
			      int(random.random()*self.vstream.height))
			tags.append((label, bb))
			label_set.add(label)

		return (sorted(list(label_set)), tags)

	def __next__(self):
		out = next(self.input_iter)
		out['tags'] = self._get_tags(out['data'])
		return out

