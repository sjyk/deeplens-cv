from deeplens.tracking.event import Metric
from deeplens.tracking.contour import KeyPoints
from deeplens.dataflow.map import Crop, GC, SkipEmpty
from deeplens.struct import build, RawVideoStream, IteratorVideoStream, VideoStream
from deeplens.extern.ffmpeg import *

import numpy as np

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

	#only handles one region
	def get_metric_region(self, pipeline):
		for index, op in enumerate(pipeline):
			if isinstance(op, Metric):
				return op.region
		return None

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

	def _getVStreamBitRate(self, vstream):
		if '.avi' in vstream.src:
			return get_bitrate(vstream.src)
		else:
			return None 

	#sqrt scale, seems to work
	def get_bitrate_scale(self, pipeline):
		baseline = 4081399.0 #roughly a streaming bitrate
		if isinstance(pipeline[0],IteratorVideoStream):
			rawbitrate = min([self._getVStreamBitRate(src) for src in pipeline[0].sources])
			
			if rawbitrate/baseline < 0.1:
				return np.sqrt(7*rawbitrate/baseline)
			else:
				return 1.0
		else:
			rawbitrate = self._getVStreamBitRate(pipeline[0])

			if rawbitrate/baseline < 0.1:
				return np.sqrt(7*rawbitrate/baseline)
			else:
				return 1.0

	def optimize(self, stream):
		pipeline = stream.lineage()

		#print(self.get_bitrate_scale(pipeline))

		#crop push down
		if self.crop_pd:
			region = self.get_metric_region(pipeline)
			if not (region is None):
				region = region + 150 #self.crop_pd_ratio

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