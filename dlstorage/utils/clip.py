import itertools

from dlstorage.xform import VideoTransform, Cut

#gets the boundaries of the clips
def clip_boundaries(start,end,size):
	start_points = list(range(start, end, size))
	tuples = [(s, min(s+(size-1),end)) for s in start_points]
	return tuples

#reconciles boundaries
def find_clip_boundaries(clip, boundaries):
	c_start,c_end = clip

	start_clip = None
	end_clip = None

	for i, bound in enumerate(boundaries):
		start,end = bound

		if start <= c_start and\
		   end >= c_start:
		   start_clip = i

		if start <= c_end and\
		   end >= c_end:
		   end_clip = i

	return start_clip, end_clip

#clip mask
def get_clip_split_merge(clip, boundaries):
	c_start,c_end = clip
	start_clip, end_clip = find_clip_boundaries(clip, boundaries)

	rtn = []
	for clip_index in range(start_clip, end_clip+1):
		b_start, b_end = boundaries[clip_index]

		rtn.append((clip_index, (max(b_start,c_start), min(b_end,c_end)) ))

	return rtn

#takes in start end clips
def materialize_clip(clip, boundaries, streams):
	execution_plan = get_clip_split_merge(clip, boundaries)
	subiterators = []

	for crop in execution_plan:
		index, bounds = crop
		subiterators.append(streams[index][Cut(*bounds)])

	#v = VideoTransform()
	return itertools.chain(*subiterators)

