"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

core.py defines the basic storage api in deeplens.
"""


class StorageManager():
	"""The StorageManager is the basic abstract class that represents a
	storage manager.
	"""
	def __init__(content_tagger):
		self.content_tagger = content_tagger


	def putFromFile(filename, target):
		"""putFromFile adds a video to the storage manager from a file
		"""
		raise NotImplemented("putFromFile not implemented")


	def putFromDevice(filenane, target):
		"""putFromDevice adds a video to the storage manager from a device
		"""
		raise NotImplemented("putFromDevice not implemented")

	#TODO
	def getIf(name, condition, clip_size):
		"""retrievies a clip of a certain size satisfying the condition
		"""
		pass