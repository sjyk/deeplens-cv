from deeplens.tracking.event import Metric
from deeplens.dataflow.map import Mask
from deeplens.struct import build, VideoStreamOperator, Box

class BlazeItOptimizer():

	def __init__(self,
				 crop_pd=True,
				 crop_pd_ratio=1.1):

		self.crop_pd = crop_pd
		self.crop_pd_ratio = crop_pd_ratio

	#only handles one region
	def get_metric_region(self, pipeline):
		for index, op in enumerate(pipeline):
			if isinstance(op, Metric):
				return op.region
		return None

	def optimize(self, stream):
		pipeline = stream.lineage()

		#crop push down, 
		#Actually does this with a mask (https://github.com/stanford-futuredata/blazeit/blob/master/blazeit/data/video_data.py)
		region = self.get_metric_region(pipeline)
		if not (region is None):
			region = region * self.crop_pd_ratio
			pipeline.insert(1, Mask(region.x0, region.y0, region.x1, region.y1))

		return build(pipeline)