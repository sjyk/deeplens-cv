"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

agg.py defines aggregation functions
"""

from deeplens.dataflow.validation import check_metrics_and_filters, countable

import time

def count(stream, keys, stats=False):
	"""Count counts the true hits of a defined event.
	"""

	#validating that the pipeline is fine
	check_metrics_and_filters(stream.lineage())
	for key in keys:
		countable(stream.lineage(), key)

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

	#actual logic is here
	counter = {}
	frame_count = 0
	now = time.time()
	for stream in streams:
		
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


def get(stream, key, frame_rate=-1):
	if frame_rate == -1:
		return [(v['frame'], v['data']) for v in stream if v[key]]
	else:
		return [( int(v['frame']/frame_rate) , v['data']) for v in stream if v[key]]


