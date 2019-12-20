"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

header.py defines the formats for the headers that denote which clips have 
what objects.
"""

#abstract header class
class Header(object):
	'''
	Just a placeholder for error checking
	'''

	def update(self, frame):
		raise NotImplemented("All headers must implement an update call")

	def getHeader(self):
		raise NotImplemented("All headers must implement a getHeader call")

	def reset(self):
		raise NotImplemented("All headers must implement a reset call")




class TimeHeader(Header):
	"""TimeHeader represents the most basic header type
	that accounts for temporally segmented clips.
	"""

	def __init__(self, offset=0):
		self.start = float("inf")
		self.offset = offset
		self.end = -1

	#keeps track of the start and the end time of the clips
	def update(self, frame):
		self.start = int(min(self.start, frame['frame']))
		self.end = int(max(self.end, frame['frame']))

	def getHeader(self):
		return {'start': self.start + self.offset, 
				'end': self.end + self.offset}

	def reset(self):
		self.start = float("inf")
		self.end = -1




class StorageHeader(Header):
	"""TimeHeader records information strorage access pattern data
	"""

	def __init__(self, keep_history = True):
		self.last_accessed = 0
		self.frequency = 0 
		self.frequency_start = datetime.now()
		self.keep_history = keep_history
		if keep_history:
			self.access_list = [] # history of access pattern
			self.access_start = datetime.now()

	#keeps track of the start and the end time of the clips
	def update(self):
		time = datetime.now()
		self.last_accessed = time
		self.frequency += 1
		if self.keep_history:
			self.access_list.append(time)

	def reset_freq(self):
		self.frequency = 0
		self.frequency_start = datetime.now()

	def has_hist(self):
		return self.keep_history

	def reset_hist(self):
		if self.keep_history:
			self.access_list = []
			self.access_start = datetime.now()
		else:
			raise HeaderError('history not stored in this header')

	def reset(self):
		self.last_accessed = 0
		self.frequency = 0
		self.frequency_start = datetime.now()
		if self.keep_history:
			self.access_list = []
			self.access_start = datetime.now() 


class ObjectHeader(TimeHeader, StorageHeader):

	"""ObjectHeader keeps track of what objects show up in
	a clip, and the
	It also keeps track of time inheriting from TimeHeader,
	and storage access inheriting from StorageHeader.
	"""

	def __init__(self, store_bounding_boxes=True, offset=0, history = True):
		self.label_set = set()
		self.bounding_boxes = []
		self.store_bounding_boxes = store_bounding_boxes
		TimeHeader.__init__(self, offset)
		StorageHeader.__init__(self, history)

	#handle the update
	def update(self, frame):
		if 'tags' not in frame:
			raise ValueError('Not compatible with this type of video')

		for label, bb in frame['tags']:
			self.label_set.add(label)
		
		if self.store_bounding_boxes:
			self.bounding_boxes.append(frame['tags'])

		TimeHeader.update(self, frame)

	def getHeader(self):
		llist = sorted(list(self.label_set))

		return {'start': self.start + self.offset, 
				'end': self.end + self.offset, 
				'label_set': llist, 
				'bounding_boxes': self.bounding_boxes,
				'last_accessed': self.last_accessed,
				'access_frequency': self.frequency,
				'frequency_start':self.frequency_start,
				'access_history': self.access_list,
				'access_history_start': self.access_start}

	def reset(self):
		self.label_set = set()
		self.bounding_boxes = []
		TimeHeader.reset(self)
		StorageHeader.reset(self)

