"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

The SimpleStorageManager class acts as a default baseline for the deeplens
system. It provides a basic file io and network io interface to put and get
videos into the storage system. 
"""
from deeplens.core import *
from deeplens.constants import *
from deeplens.struct import *
from deeplens.simple_manager.videoio import *
from deeplens.simple_manager.file import *
from deeplens.header import *
from deeplens.dataflow.map import *
from deeplens.error import *

import os


class SimpleStorageManager(StorageManager):
	"""The SimpleStorageManger stores videos as files that are temporally partitioned
	   into equiwidth segments.
	"""

	DEFAULT_ARGS = {'encoding': GSC, 'size': -1, 'limit': -1, 'sample': 1.0, 'offset': 0}


	def __init__(self, content_tagger, basedir):
		'''Every simplestoragemanager takes as input a content_tagger and a 
		   basedir for storage.
		'''
		self.content_tagger = content_tagger
		self.basedir = basedir
		self.videos = set()

		if not os.path.exists(basedir):
			try:
				os.makedirs(basedir)
			except:
				raise ManagerIOError("Cannot create the directory: " + str(basedir))


	def put(self, filename, target, args=DEFAULT_ARGS):
		'''Put takes in a file on disk and writes it as the target name with the arguments
		'''
		self.doPut(filename, target, args)


	def get(self, name, condition, clip_size):
		'''Get takes in a name and a condition that the system tries a best effort to push down
		'''
		return self.doGet(name, condition, clip_size)


	def delete(self, name):
		'''Delete deletes a clip from the storage engine
		'''
		physical_clip = os.path.join(self.basedir, name)

		if name in self.videos:
			self.videos.remove(name)

		delete_video_if_exists(physical_clip)



	def list(self):
		'''List lists all the clips in the engine
		'''
		return list(self.videos)


	def size(self, name):
		'''Returns the storage size of a clip
		'''
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

	


	def doGet(self, name, condition, clip_size):
		"""retrievies a clip of a certain size satisfying the condition
		"""
		if name not in self.videos:
			raise VideoNotFound(name + " not found in " + str(self.videos))


		physical_clip = os.path.join(self.basedir, name)

		return read_if(physical_clip, condition, clip_size)
	


