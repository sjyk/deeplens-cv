import time

def count(stream, keys, stats=False):
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