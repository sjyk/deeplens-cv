"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

agg.py defines aggregation functions
"""

from deeplens.dataflow.validation import check_metrics_and_filters, countable
from deeplens.struct import IteratorVideoStream
from deeplens.dataflow.xform import Null

import time
import itertools

def count(stream, keys, stats=False):
	"""Count counts the true hits of a defined event.
	"""

	#validating that the pipeline is fine, fix
	#check_metrics_and_filters(stream.lineage())
	#for key in keys:
	#	countable(stream.lineage(), key)

	#actual logic is here
	counter = {}
	frame_count = 0
	now = time.time()
	for frame in stream:
		frame_count += 1

		for key in keys:
			try:
				counter[key] = counter.get(key,0) + frame[key]
			except:
				pass

	if not stats:
		return counter
	else:
		return counter, {'frames': frame_count, \
						 'elapsed': (time.time() - now)}

def counts(streams, keys, stats=False):
	"""Count counts the true hits of a defined event.
	"""

	#validating that the pipeline is fine
	check_metrics_and_filters(streams[0].lineage())
	for key in keys:
		countable(streams[0].lineage(), key)

	return count(IteratorVideoStream(itertools.chain(*streams)), keys, stats)
	"""
	#actual logic is here
	counter = {}
	frame_count = 0
	now = time.time()

	print(len(streams))

	for stream in streams:
		
		now2 = time.time()

		for frame in stream:
			frame_count += 1

			for key in keys:
				try:
					counter[key] = counter.get(key,0) + frame[key]
				except:
					pass

		print((time.time()-now2))

	if not stats:
		return counter
	else:
		return counter, {'frames': frame_count, \
						 'elapsed': (time.time() - now)}
	"""