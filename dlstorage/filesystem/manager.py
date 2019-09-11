"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

manager.py defines the basic storage api in deeplens.
"""
from dlstorage.core import *
from dlstorage.constants import *
from dlstorage.stream import *
from dlstorage.filesystem.videoio import *
from dlstorage.filesystem.ffmpeg import *
from dlstorage.header import *
from dlstorage.xform import *
from dlstorage.error import *

from multiprocessing import Pool

import os

class FileSystemStorageManager(StorageManager):
	"""The FileSystemStorageManger stores videos as files
	"""
	DEFAULT_ARGS = {'encoding': GSC, 'size': -1, 'limit': -1, 'sample': 1.0, 'offset': 0}

	def __init__(self, content_tagger, basedir):
		self.content_tagger = content_tagger
		self.basedir = basedir
		self.videos = set()
		self.threads = None
		self.STORAGE_BLOCK_SIZE = 60 #in seconds

		if not os.path.exists(basedir):
			try:
				os.makedirs(basedir)
			except:
				raise ManagerIOError("Cannot create the directory: " + str(basedir))


	def put(self, filename, target, args=DEFAULT_ARGS):

		#optimized for big puts
		if not self._parallelEligible(filename, args['limit']):
			self.doPut(filename, target, args)
		else:
			block_put(self,filename, target, block_size=self.STORAGE_BLOCK_SIZE, args=args)


	def _parallelEligible(self, filename, limit):
		if 'http://' in filename or filename == 0:
			return False
		elif get_duration(filename) < 2*self.STORAGE_BLOCK_SIZE:
			return False
		elif limit != -1:
			return False
		else:
			return True

	def doPut(self, filename, target, args=DEFAULT_ARGS):
		"""putFromFile adds a video to the storage manager from a file
		"""
		v = VideoStream(filename, args['limit'])
		v = v[Sample(args['sample'])]
		v = v[self.content_tagger]

		physical_clip = os.path.join(self.basedir, target)
		delete_video_if_exists(physical_clip)

		if args['size'] == -1:
			write_video(v, \
					    physical_clip, args['encoding'], \
					    ObjectHeader(offset=args['offset']))
		else:
			write_video_clips(v, \
							  physical_clip, \
							  args['encoding'], \
							  ObjectHeader(offset=args['offset']), \
							  args['size'])

		self.videos.add(target)

	def setThreadPool(self, workers):
		if workers == None:
			self.threads = None
		else:
			self.threads = workers

	def get(self, name, condition, clip_size):
		if name in self.videos:
			return self.doGet(name, condition, clip_size)

		return block_get(self, name, condition, clip_size)

	def doGet(self, name, condition, clip_size):
		"""retrievies a clip of a certain size satisfying the condition
		"""
		if name not in self.videos:
			raise VideoNotFound(name + " not found in " + str(self.videos))


		physical_clip = os.path.join(self.basedir, name)

		if self.threads == None:
			return read_if(physical_clip, condition, clip_size, threads=None)
		else:
			return read_if(physical_clip, condition, clip_size, threads=Pool(self.threads))


	def delete(self, name):
		physical_clip = os.path.join(self.basedir, name)

		if name in self.videos:
			self.videos.remove(name)

		delete_video_if_exists(physical_clip)


	def list(self):
		return list(self.videos)


	def size(self, name):
		seq = 0
		size = 0
		physical_clip = os.path.join(self.basedir, name)

		while True:

			try:
				file = add_ext(physical_clip, '.seq', seq) 
				size += sum(os.path.getsize(os.path.join(file,f)) for f in os.listdir(file))
				seq += 1

			except FileNotFoundError:
				break

		return size