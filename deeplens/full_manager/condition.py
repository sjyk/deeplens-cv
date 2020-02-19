"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

condition.py defines a implemented Condition class and related functions

"""

from deeplens.full_manager.full_videoio import query_label, query_clip
from deeplens.struct import Box

#condition is a predicate push down condition
#implements filter and crop only
class Condition():

	def __init__(self,
				 filter,            # filter is a string
				 crop=None,         # crop is a Box object
				 resolution=1.0,
				 sampling=1.0):

		self.filter = filter
		self.crop = crop
		self.resolution = resolution
		self.sampling = sampling

	def query(self, conn, video_name):
		clips = query_label(conn, self.filter, video_name)
		clip_ids = [label[1] for label in clips]

		if self.crop != None:
			assert isinstance(self.crop, Box)
			clip_ids = self._crop(conn, video_name, clip_ids)

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
