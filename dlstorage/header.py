"""header.py defines the formats for the headers that
denote which clips have what objects.
"""

#abstract header class
class Header(object):
	'''
	Just a place holder for error checking
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

	def __init__(self):
		self.start = float("inf")
		self.end = -1

	#keeps track of the start and the end time of the clips
	def update(self, frame):
		self.start = int(min(self.start, frame['frame']))
		self.end = int(max(self.end, frame['frame']))

	def getHeader(self):
		llist = sorted(list(self.label_set))
		return {'start': self.start, 
				'end': self.end}

	def reset(self):
		self.start = float("inf")
		self.end = -1




class ObjectHeader(TimeHeader):

	"""ObjectHeader keeps track of what objects show up in
	a clip. It also keeps track of time inheriting from TimeHeader
	"""

	def __init__(self):
		self.label_set = set()
		self.bounding_boxes = []
		super(ObjectHeader, self).__init__()

	#handle the update
	def update(self, frame):
		if 'tags' not in frame:
			raise ValueError('Not compatible with this type of video')

		for label, bb in frame['tags']:
			self.label_set.add(label)
		
		self.bounding_boxes.append(frame['tags'])

		super(ObjectHeader, self).update(frame)


	def getHeader(self):
		llist = sorted(list(self.label_set))
		return {'start': self.start, 
				'end': self.end, 
				'label_set': llist, 
				'bounding_boxes': self.bounding_boxes}

	def reset(self):
		self.label_set = set()
		self.bounding_boxes = []
		super(ObjectHeader, self).reset()



#language constructs
def TRUE(x):
	return True

def FALSE(x):
	return False

def hasLabel(l):
	return lambda x: l in x['label_set']

def startsBefore(time):
	return lambda x: time >= x['start']

def startsAfter(time):
	return lambda x: time < x['start']

def endsBefore(time):
	return lambda x: time >= x['end']

def endsAfter(time):
	return lambda x: time < x['end']

def AND(f1, f2):
	return lambda x: f1(x) and f2(x)

def OR(f1, f2):
	return lambda x: f1(x) or f2(x)

