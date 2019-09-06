"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

manager.py defines the basic storage api in deeplens.
"""
from dlstorage.core import *
from dlstorage.constants import *
from dlstorage.stream import *
from dlstorage.filesystem.videoio import *
from dlstorage.header import *
from dlstorage.xform import *

import os

class FileSystemStorageManager(StorageManager):
	"""The FileSystemStorageManger stores videos as files
	"""
	DEFAULT_ARGS = {'encoding': GSC, 'size': -1, 'limit': -1, 'sample': 1.0}

	def __init__(self, content_tagger, basedir):
		self.content_tagger = content_tagger
		self.basedir = basedir
		self.videos = set()

		if not os.path.exists(basedir):
			os.makedirs(basedir)

	def put(self, filename, target, args=DEFAULT_ARGS):
		"""putFromFile adds a video to the storage manager from a file
		"""
		v = VideoStream(filename, args['limit'])
		v = v[Sample(args['sample'])]
		v = v[self.content_tagger]

		physical_clip = os.path.join(self.basedir, target)
		delete_video_if_exists(physical_clip)

		if args['size'] == -1:
			write_video(v, physical_clip, args['encoding'], ObjectHeader())
		else:
			write_video_clips(v, physical_clip, args['encoding'], ObjectHeader(), args['size'])

		self.videos.add(target)


	def get(self, name, condition, clip_size, threads=None):
		"""retrievies a clip of a certain size satisfying the condition
		"""
		if name not in self.videos:
			return None

		physical_clip = os.path.join(self.basedir, name)

		return read_if(physical_clip, condition, clip_size, threads=threads)

	def delete(self, name):
		physical_clip = os.path.join(self.basedir, name)
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


