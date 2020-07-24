"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

condition.py defines a implemented Condition class and related functions

"""

from deeplens.full_manager.full_videoio import query_label, query_clip, query_everything
from deeplens.struct import Box

import json

#condition is a predicate push down condition
#implements filter and crop only
class Condition():

	def __init__(self,
				 label=None,       # label is a string
				 crop=None,        # crop is a Box object
				 resolution=1.0,
				 sampling=1.0,
				 custom_filter=None):

		self.label = label
		self.crop = crop
		self.resolution = resolution
		self.sampling = sampling
		self.custom_filter = custom_filter

	def query_everything(self, conn, video_name, backgorund = False):
		if backgorund:
			raise NotImplementedError("Not supported yet. Please set background = True!")
		clips = query_everything(conn, video_name)
		clip_ids = [label[1] for label in clips]
		return clip_ids


	def query(self, conn, video_name):
		if self.label == None:
			clip_ids = self.query_everything(conn, video_name)
			filtered_ids = self.custom_filter(conn, video_name)
			return list(set(clip_ids).intersection(filtered_ids))

		clips = query_label(conn, self.label, video_name)
		clip_ids = [label[1] for label in clips]

		if self.crop != None:
			assert isinstance(self.crop, Box)
			clip_ids = self._crop(conn, video_name, clip_ids)

		if self.custom_filter != None:
			assert callable(self.custom_filter)
			filtered_ids = self.custom_filter(conn, video_name)
			clip_ids = list(set(clip_ids).intersection(filtered_ids))

		return clip_ids


	def _crop(self, conn, video_name, clip_ids):
		cropped_ids = []
		for id in clip_ids:
			clip = query_clip(conn, id, video_name)
			clip_position = Box(clip[0][4], clip[0][5],
								clip[0][4] + clip[0][7], clip[0][5] + clip[0][6])
			if self.crop.contains(clip_position) or \
					self.crop.intersect_area(clip_position) / clip_position.area() >= 0.7:
				cropped_ids.append(id)
		return cropped_ids


	def serialize(self):
		data = {'label' : self.label,
				'crop' : self.crop,
				'resolution' : self.resolution, 
				'sampling' : self.sampling, 
				'custom_filter' : self.custom_filter}

		return json.dumps(data)



def deserialize(serialized):
	return Condition(**json.loads(serialized))



