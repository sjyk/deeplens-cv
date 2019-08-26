

class ObjectHeader():

	def __init__(self):
		self.label_set = set()
		self.bounding_boxes = []
		self.start = float("inf")
		self.end = -1

	def update(self, frame):
		self.start = int(min(self.start, frame['frame']))
		self.end = int(max(self.end, frame['frame']))

		if 'tags' not in frame:
			raise ValueError('Not compatible with this type of video')

		for label, bb in frame['tags']:
			self.label_set.add(label)
		
		self.bounding_boxes.append(frame['tags'])


	def getHeader(self):
		llist = sorted(list(self.label_set))
		return {'start': self.start, 
				'end': self.end, 
				'label_set': llist, 
				'bounding_boxes': self.bounding_boxes}



class TimeHeader():

	def __init__(self):
		self.start = float("inf")
		self.end = -1

	def update(self, frame):
		self.start = int(min(self.start, frame['frame']))
		self.end = int(max(self.end, frame['frame']))

	def getHeader(self):
		llist = sorted(list(self.label_set))
		return {'start': self.start, 
				'end': self.end}