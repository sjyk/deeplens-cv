"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered.py defines file storage operations. These operations allows for
complex file storage systems such as tiered storage, and easy file
movement and access.

"""

from dlstorage.core import *

class TieredFileStorageManager(StorageManager):
	""" TieredStorageManager is the abstract class that represents a 3 tiered
	storage manager, with a cache and external stroage
	"""

	def __init__(self, content_tagger):
		self.content_tagger = content_tagger

	def put(self, filename, target, args, in_extern_storage):
		"""put adds a video to the storage manager from a file. It should either add
			the video to disk, or a reference in disk to deep storage.
		"""
		raise NotImplemented("putFromFile not implemented")

	def get(self, name, condition, clip_size):
		"""retrievies a clip of a certain size satisfying the condition.
		If the clip was in external storage, get moves it to disk.
		"""
		raise NotImplemented("get not implemented")

	def delete(self, name):
		raise NotImplemented("delete not implemented")

	def list(self):
		raise NotImplemented("list() not implemented")
	
	def setThreadPool(self):
		raise ValueError("This storage manager does not support threading")

	def size(self, name):
		""" Note: implementations need to be careful to specify 
		if extern space or cache space is counted. Default is 
		that it isn't counted
		"""
		raise NotImplemented("size() not implemented")

	def moveToExtern(self, name, condition): 
		""" Move clips that fulfil the condition to disk
		"""
		raise NotImplemented("isExtern() not implemented")

	def moveFromExtern(self, name, condition): 
		""" Move clips that fulfil the condition to disk
		"""
		raise NotImplemented("isExtern() not implemented")	

	def isExtern(self, name, condition):
		""" Default: returns True if any of the files that meet the requirements
		is in external storage
		"""
		raise NotImplemented("isExtern() not implemented")	
