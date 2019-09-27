import time

def count(stream, key, stats=False):
	counter = 0
	frame_count = 0
	now = time.time()
	for frame in stream:
		frame_count += 1
		try:
			counter += frame[key]
		except:
			pass

	if not stats:
		return counter
	else:
		return counter, {'frames': frame_count, \
						 'elapsed': (time.time() - now)}