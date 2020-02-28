from deeplens.tracking.event import Metric
from deeplens.dataflow.map import Crop, GC, SkipEmpty
from deeplens.struct import build

class DeepLensOptimizer():

	def __init__(self,
				 crop_pd=True,
				 crop_pd_ratio=1.1,
				 skip_empty=True,
				 gc=True):

		self.crop_pd = crop_pd
		self.crop_pd_ratio = crop_pd_ratio
		self.gc = gc
		self.skip_empty = skip_empty

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

	def optimize(self, stream):
		pipeline = stream.lineage()

		#crop push down
		if self.crop_pd:
			region = self.get_metric_region(pipeline)
			if not (region is None):
				region = region * self.crop_pd_ratio
				pipeline.insert(1,Crop(region.x0, region.y0, region.x1, region.y1))

				if self.skip_empty:
					pipeline.insert(2,SkipEmpty())

		if self.gc:
			index = self.get_metric_index(pipeline)
			if not (index is None):
				pipeline.insert(index,GC())



		return build(pipeline) 