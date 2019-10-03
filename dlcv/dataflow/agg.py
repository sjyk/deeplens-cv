"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

agg.py defines aggregation functions
"""

from dlcv.dataflow.validation import check_metrics_and_filters, countable

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