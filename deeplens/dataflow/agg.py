"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

agg.py defines aggregation functions
"""

from deeplens.dataflow.validation import check_metrics_and_filters, countable
from deeplens.struct import IteratorVideoStream
from deeplens.dataflow.xform import Null

import logging
import time
import itertools

def count(stream, keys, stats=False):
	"""Count counts the true hits of a defined event.
	"""

	#actual logic is here
	counter = {}
	frame_count = 0
	now = time.time()
	for frame in stream:
		frame_count += 1

		if frame_count == 1:
			logging.info("Processing first frame of stream")

		for key in keys:
			try:
				counter[key] = counter.get(key,0) + frame[key]
			except:
				pass

	# profiling
	for obj in stream.lineage():
		if hasattr(obj, "time_elapsed"):
			logging.info("%s: %s" % (type(obj).__name__, obj.time_elapsed))
		else:
			logging.info("%s time not measured" % type(obj).__name__)

	if not stats:
		return counter
	else:
		return counter, {'frames': frame_count, \
						 'elapsed': (time.time() - now)}

def counts(streams, keys, stats=False):
	"""Count counts the true hits of a defined event.
	"""
	stream = IteratorVideoStream(itertools.chain(*streams), streams)

	lineage = []
	for s in streams:
		lineage.extend(s.lineage())

	stream.global_lineage = lineage

	return count(stream, keys, stats)


def get(stream, key, frame_rate=-1):
	if frame_rate == -1:
		return [(v['frame'], v['data']) for v in stream if v[key]]
	else:
		return [( int(v['frame']/frame_rate) , v['data']) for v in stream if v[key]]
