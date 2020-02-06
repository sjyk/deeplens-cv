"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

header.py defines the formats for the headers that denote which clips have 
what objects.
"""

from datetime import datetime
from deeplens.error import HeaderError
from deeplens.struct import *
import json

import cv2

def _check_keys(header, keys):
	'''
	Check that keys are in header
	'''
	return all(key in header for key in keys)

#abstract header class
class HeaderFunct(object):
	'''
	Just a placeholder for error checking
	'''
	def __init__(self, header):
		self.header = header
		self.keys = []

	def update(self, frame):
		raise NotImplemented("All headers must implement an update call")

	def reset(self):
		raise NotImplemented("All headers must implement a reset call")

class ClipHeader(HeaderFunct):
	"""TimeHeader represents the most basic header type
	that accounts for temporally segmented clips.
	"""

	def __init__(self, \
				header, \
				forced_new = False, \
				offset = 0, \
				origin_x = 0, \
				origin_y = 0, \
				width = -1, \
				height = -1, \
				clip_id = -1, \
				video_name = '', \
				has_label = False, \
				video_ref = '', \
				background = False,
				):
		super().__init__(header, forced_new)
		self.keys.extend(['start', 'end', 'clip_id', 'video_name', \
							'origin_x', 'origin_y', 'height', 'width', 'has_label', \
							'video_ref, translation'])
		if forced_new or not _check_keys(self.header, self.keys):
			self.offset = offset
			self.translations = []
			self.header['start'] = offset
			self.header['height'] = height
			self.header['width'] = width
			self.header['orgin_x'] = origin_x
			self.header['origin_y'] = origin_y
			self.header['end'] = -1
			self.header['clip_id'] = clip_id
			self.header['video_name'] = video_name
			self.header['has_label'] = has_label
			self.header['video_ref'] = video_ref
			self.header['translation'] = json.dumps(self.translation)
			self.header['background'] = background
		else:
			self.offset = header['start']
			self.translations = json.loads(header['translation'])

	def _check_origin(curr, new):
		if curr[0] != new[0] or curr[1] != new[1]:
			return True
		else:
			return False
	#keeps track of the start and the end time of the clips
	def update(self, index, new_orign = None):
		frame = index + self.offset
		self.header['start'] = int(min(self.header['start'], frame))
		self.header['end'] = int(max(self.header['end'], frame))
		if new_origin:
			if len(self.translations) == 0 and self._check_origin(self.header['origin'], new_origin):
				self.translations.append(stream.origin)
				self.header['translation'] = json.dumps(self.translations)
			elif self._check_origin(self.translations[-1], new_origin):
				self.translations.append(new_origin)
				self.header['translation'] = json.dumps(self.translations)

	def reset(self):
		self.header['start'] = float("inf")
		self.header['end'] = -1


class TimeHeader(HeaderFunct):
	"""TimeHeader represents the most basic header type
	that accounts for temporally segmented clips.
	"""

	def __init__(self, header, forced_new = False, offset=0):
		super().__init__(header, forced_new)
		self.keys.extend(['start', 'offset', 'end'])
		if forced_new or not _check_keys(self.header, self.keys):
			self.header['start'] = offset
			self.header['end'] = -1

	#keeps track of the start and the end time of the clips
	def update(self, frame):
		self.header['start'] = int(min(self.header['start'], frame['frame']))
		self.header['end'] = int(max(self.header['end'], frame['frame']))

	def reset(self):
		self.header['start'] = float("inf")
		self.header['end'] = -1

class StorageHeader(HeaderFunct):
	"""StorageHeader records information strorage access pattern data
	"""

	def __init__(self, header, forced_new = False, keep_history = True):
		super().__init__(header, forced_new)
		self.keys.extend(['last_accessed', 'frequency', 'frequency_start', 'keep_history',\
						 'access_list', 'access_start'])
		if new or not _check_keys(self.header, self.keys):
			self.header['last_accessed'] = 0
			self.header['frequency'] = 0 
			self.header['frequency_start'] = datetime.now()
			self.header['keep_history'] = keep_history
			self.header['access_list'] = [] # history of access pattern
			self.header['access_start'] = datetime.now()

	#keeps track of the start and the end time of the clips
	def update(self):
		self.header['time'] = datetime.now()
		self.header['last_accessed'] = time
		self.header['frequency'] += 1
		if self.header['keep_history']:
			self.header['access_list'].append(time)

	def reset_freq(self):
		self.header['frequency'] = 0
		self.header['frequency_start'] = datetime.now()

	def has_hist(self):
		return self.header['keep_history']

	def reset_hist(self):
		if self.header['keep_history']:
			self.header['access_list'] = []
			self.header['access_start'] = datetime.now()
		else:
			raise HeaderError('history not stored in this header')

	def reset(self):
		self.header.last_accessed = 0
		self.reset_freq()
		if self.keep_history:
			self.reset_hist()


class LabelHeader(HeaderFunct):
	"""ObjectHeader keeps track of the labels over a video
	segment and clip ids if they are in another clip are needed
	"""

	def __init__(self, header, forced_new = False, store_bounding_boxes=True):
		super().__init__(header, forced_new)
		self.keys.extend(['bound_boxes', 'store_bound_boxes'])
		if new or not _check_keys(self.header):
			self.header['label_set'] = set()
			self.header['bounding_boxes'] = []
			self.header['store_bounding_boxes'] = store_bounding_boxes

	#handle the update
	def update(self, frame, bb):
		if 'tags' not in frame:
			raise ValueError('Not compatible with this type of video')

		for label, bb in frame['tags']:
			self.header['label_set'].add(label)
		
		if self.store_bounding_boxes:
			self.header['bounding_boxes'].append(frame['tags'])

		TimeHeader.update(self, frame)

	def reset(self):
		self.header['label_set'] = set()
		self.header['bounding_boxes'] = []
		super().reset()

class BoxHeader(HeaderFunct):
	"""ObjectHeader keeps track of bounding boxes over frames
	"""