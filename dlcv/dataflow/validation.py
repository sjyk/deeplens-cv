from dlcv.tracking.event import *

class UsedBeforeDefined(Exception):
   """Raised when opencv cannot open a video"""
   pass

class RegionError(Exception):
   """Raised when opencv cannot open a video"""
   pass



def get_metric_map(pipeline):
	metric = {}
	for index, op in enumerate(pipeline):
		if isinstance(op, Metric):
			metric[op.name] = index
	return metric

def get_filter_map(pipeline):
	filter = {}
	for index, op in enumerate(pipeline):
		if isinstance(op, Filter):
			filter[op.name] = index
	return filter

def check_metrics_and_filters(pipeline):
	ms = get_metric_map(pipeline)
	fs = get_filter_map(pipeline)

	for fil in fs:
		if fil not in ms:
			raise UsedBeforeDefined(fil + " is not defined")
		elif ms[fil] > fs[fil]:
			raise UsedBeforeDefined(fil + " is defined after filter")

	return True

def countable(pipeline, name):
	fs = get_filter_map(pipeline)
	if name not in fs:
		raise UsedBeforeDefined(name + " is not defined")
	return True

