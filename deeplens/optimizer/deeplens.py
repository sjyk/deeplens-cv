from deeplens.tracking.event import Metric
from deeplens.tracking.contour import KeyPoints
from deeplens.dataflow.map import Crop, GC, SkipEmpty
from deeplens.struct import build, RawVideoStream, IteratorVideoStream, VideoStream
from deeplens.extern.ffmpeg import *
from deeplens.full_manager.condition import Condition

import numpy as np
import datetime

class DeepLensOptimizer():

	def __init__(self,
				 crop_pd=True,
				 crop_pd_ratio=1.1,
				 raw_vid_opt = True,
				 skip_empty=True,
				 adaptive_blur=True,
				 gc=True):


		self.crop_pd = crop_pd
		self.crop_pd_ratio = crop_pd_ratio
		self.raw_vid_opt = raw_vid_opt
		self.gc = gc
		self.skip_empty = skip_empty
		self.adaptive_blur = adaptive_blur

		self.start = datetime.datetime.now()

		self.usage_stats = {}

	#only handles one region
	def get_metric_region(self, pipeline):
		for index, op in enumerate(pipeline):
			if isinstance(op, Metric):
				return op.region
		return None

	def get_size_est(self, src):
		width = get_width(src)
		height = get_height(src)
		duration = get_duration(src)

		return width*height*duration*3

	#only handles one region
	def get_metric_index(self, pipeline):
		for index, op in enumerate(pipeline):
			if isinstance(op, Metric):
				return index
		return None

	def get_keypoint_op(self, pipeline):
		for index, op in enumerate(pipeline):
			if isinstance(op, KeyPoints):
				return op

		return None

	def _getVStreamBitRate(self, src):
		if '.avi' in src:
			return get_bitrate(src)
		else:
			return np.inf 

	#sqrt scale, seems to work
	def get_bitrate_scale(self, pipeline):
		baseline = 4081399.0 #roughly a streaming bitrate
		if isinstance(pipeline[0],IteratorVideoStream):
			rawbitrate = min([self._getVStreamBitRate(src) for src in pipeline[0].sources])
			
			if rawbitrate/baseline < 0.1:
				return np.sqrt(7*rawbitrate/baseline)
			else:
				return 1.0
		elif isinstance(pipeline[0],RawVideoStream):
			return 1.0
		else:
			rawbitrate = self._getVStreamBitRate(pipeline[0])

			if rawbitrate/baseline < 0.1:
				return np.sqrt(7*rawbitrate/baseline)
			else:
				return 1.0

	def get_source_clips(self, pipeline):

		if isinstance(pipeline[0],IteratorVideoStream):
			return [src for src in pipeline[0].sources]
		else:
			return [pipeline[0].src]

	def optimize(self, stream):
		pipeline = stream.lineage()

		#print(self.get_bitrate_scale(pipeline))

		#crop push down
		if self.crop_pd:
			region = self.get_metric_region(pipeline)

			region.x0 = region.x0*pipeline[0].scale
			region.y0 = region.y0*pipeline[0].scale
			region.x1 = region.x1*pipeline[0].scale
			region.y1 = region.y1*pipeline[0].scale

			if not (region is None):
				region = region*self.crop_pd_ratio

				for file in self.get_source_clips(pipeline):
					area = (region.x1 - region.x0)*(region.y1 - region.y0)

					self.usage_stats[file] = self.usage_stats.get(file,np.array([0.0,0.0,0.0])) + np.array([area, 1,0])  
					self.usage_stats[file][2] = (datetime.datetime.now()-self.start).total_seconds()

				pipeline.insert(1,Crop(region.x0*pipeline[0].scale, region.y0*pipeline[0].scale, region.x1*pipeline[0].scale, region.y1*pipeline[0].scale))

				if self.skip_empty:
					pipeline.insert(2,SkipEmpty())

		"""
		if self.raw_vid_opt:

			if isinstance(pipeline[0],RawVideoStream):
				pipeline[0].set_channels(0)

			if isinstance(pipeline[0],IteratorVideoStream):
				for stream in pipeline[0].sources:
					if isinstance(stream,RawVideoStream):
						stream.set_channels(0)
		"""

		if self.adaptive_blur:
			kpopt = self.get_keypoint_op(pipeline)
			blur = int(self.get_bitrate_scale(pipeline)*kpopt.blur)


			if pipeline[0].scale < 0.8:
				blur = min(3, blur)
			elif pipeline[0].scale < 0.4:
				blur = min(1, blur)
			else:
				blur = min(5, blur)

			kernel_blurs = range(1,32,2)
			closest = [ (abs(v-blur),i) for i, v in enumerate(kernel_blurs)]
			closest.sort()

			kpopt.blur = kernel_blurs[closest[0][1]]
			kpopt.area_thresh = kpopt.area_thresh * pipeline[0].scale * pipeline[0].scale
			
		if self.gc:
			index = self.get_metric_index(pipeline)
			if not (index is None):
				pipeline.insert(index,GC())

		return build(pipeline)

	def clip_filter(self, filename):

		def do_filter(conn, video_name):
			c = conn.cursor()

			#filename_proc = filename.split('/')[1]

			#print("SELECT clip_id FROM clip WHERE video_ref=%%sfilename_proc" % (str(filename)))
			c.execute("SELECT clip_id FROM clip WHERE video_ref = '%s'" % (str(filename)))

			return [cl[0] for cl in c.fetchall()]

		return do_filter 

	def _cacheClip(self, manager, clip):
		videos = manager.list()
		for video in videos:
			cache_condition = self.clip_filter(clip)
			manager.cache(video, Condition(custom_filter=cache_condition))

	def cacheLRU(self, manager, budget):
		videos = manager.list()
		current_level = sum(manager.size(v) for v in videos)

		clips_to_cache = [ (v[2],k) for k,v in self.usage_stats.items() if '.npz' not in k]
		clips_to_cache.sort(reverse=True) #sort by time

		for _ , clip in clips_to_cache:
			if current_level >= budget:
				break

			self._cacheClip(manager, clip)
			current_level = sum(manager.size(v) for v in videos)

			#print(current_level)


	def cacheKnapsack(self, manager, budget):
		videos = manager.list()
		current_level = sum(manager.size(v) for v in videos)

		def _benefit_model(stats, constant=113000.0):
			return (1 - stats[0]/constant)*stats[1]


		clips_to_cache = [ (_benefit_model(v),k) for k,v in self.usage_stats.items() if '.npz' not in k]
		#clips_to_cache.sort(reverse=True) #sort by time

		for value , clip in clips_to_cache:
			#print(value)
			if current_level >= budget or value < 0:
				break

			self._cacheClip(manager, clip)
			current_level = sum(manager.size(v) for v in videos)
			



