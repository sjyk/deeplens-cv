"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

videoio.py uses opencv (cv2) to read and write files to disk. It contains
primitives to encode and decode archived and regular video formats.
"""

from dlstorage.filesystem.file import *
from dlstorage.constants import *
from dlstorage.stream import *
from dlstorage.header import ObjectHeader
from dlstorage.utils.clip import *

import cv2
import os
import time
import shutil

def write_video(vstream, \
				output, \
				encoding, \
				header,
				scratch= DEFAULT_TEMP, \
				frame_rate=DEFAULT_FRAME_RATE, \
				header_cmp=RAW):
	"""write_video takes a stream of video and writes
	it to disk. It includes the specified header 
	information as a part of the video file.

	Args:
		vstream - a videostream or videotransform
		output - output file
		header - a header object that constructs the right
		header information
		scratch - temporary space to use
		frame_rate - the frame_rate of the video
		header_cmp - compression if any on the header
	"""

	# Define the codec and create VideoWriter object
	fourcc = cv2.VideoWriter_fourcc(*encoding)
	start = True
	tags = []

	#tmp file for the video
	r_name = get_rnd_strng()
	seg_name = os.path.join(scratch, r_name)

	file_name = add_ext(seg_name, AVI)

	global_time_header = ObjectHeader(store_bounding_boxes=False)
	
	for frame in vstream:

		if start:

			out = cv2.VideoWriter(file_name,
								  fourcc, 
								  frame_rate, 
								  (vstream.width, vstream.height),
								  True)
			start = False

		header.update(frame)

		out.write(frame['data'])
		tags.append(frame['tags'])

		global_time_header.update(frame)


	seg_start_file = write_block(global_time_header.getHeader(), \
									None ,\
									add_ext(output, '.start'))

	return [seg_start_file, \
			build_fmt_file(header.getHeader(), \
						   file_name, \
						   scratch, \
						   add_ext(output, '.seq', 0), 
						   header_cmp, \
						   RAW,\
						   seg_name)]






def write_video_clips(vstream, \
						output, \
						encoding, \
						header,
						clip_size,
						scratch = DEFAULT_TEMP, \
						frame_rate=DEFAULT_FRAME_RATE, \
						header_cmp=RAW):
	"""write_video_clips takes a stream of video and writes
	it to disk. It includes the specified header 
	information as a part of the video file. The difference is that 
	it writes a video to disk from a stream in clips of a specified 
	size

	Args:
		vstream - a videostream or videotransform
		output - output file
		header - a header object that constructs the right
		header information
		clip_size - how many frames in each clip
		scratch - temporary space to use
		frame_rate - the frame_rate of the video
		header_cmp - compression if any on the header
	"""

	# Define the codec and create VideoWriter object
	counter = 0
	seq = 0

	output_files = []

	global_time_header = ObjectHeader(store_bounding_boxes=False)
	#clip_size = min(global_time_header.end, clip_size)

	for frame in vstream:

		if counter == 0:
			#tmp file for the video
			r_name = get_rnd_strng()
			seg_name = os.path.join(scratch, r_name)

			file_name = add_ext(seg_name, AVI, seq)
			fourcc = cv2.VideoWriter_fourcc(*encoding)

			out = cv2.VideoWriter(file_name,
								  fourcc, 
								  frame_rate, 
								  (vstream.width, vstream.height),
								  True)


		out.write(frame['data'])
		header.update(frame)
		global_time_header.update(frame)
		counter += 1

		if counter == clip_size:
			output_files.append(build_fmt_file(header.getHeader(), \
												file_name, \
												scratch, \
												add_ext(output, '.seq', seq), \
												header_cmp, \
												RAW, 
												seg_name))

			header.reset()
			out.release()
			
			counter = 0
			seq += 1


	if counter != 0:
		output_files.append(build_fmt_file(header.getHeader(), \
												file_name, \
												scratch, \
												add_ext(output, '.seq', seq), \
												header_cmp, \
												RAW, 
												seg_name))

		header.reset()
		out.release()
	

	output_files.append(write_block(global_time_header.getHeader(), \
									None ,\
									add_ext(output, '.start')))

	return output_files


#delete a video
def delete_video_if_exists(output):

	start_file = add_ext(output, '.start')

	if not os.path.exists(start_file):
		return

	os.remove(start_file)
	seq = 0

	while True:
		try:
			shutil.rmtree(add_ext(output, '.seq', seq))
			seq += 1
		except FileNotFoundError:
			break

#gets a file of a particular index
def _file_get(file):
	parsed = ncpy_unstack_block(file)

	if '.head' in parsed[0]:
		head, video = 0, 1
	else:
		head, video = 1, 0

	header = unstack_block(parsed[head], DEFAULT_TEMP, compression_hint=RAW)
	header_data = read_block(header[0])
	return header_data, parsed[video]


def _all_files(output):
	rtn = []

	seq = 0
	while True:
		file = add_ext(output, '.seq', seq)

		if not os.path.exists(file):
			return rtn

		rtn.append(file)
		seq += 1

	return rtn




#counter using the start and end
def read_if(output, condition, clip_size=5, scratch = DEFAULT_TEMP, threads=None):
	"""read_if takes a written archive file and reads only
	those video clips that satisfy a certain header condition.

	Args:
		output (string) - archive file
		condition (lambda) - a condition on the header content
		scratch (string) - a temporary file path
	"""
	seq = 0
	

	#read the meta data
	seg_start_data = read_block(add_ext(output, '.start'))
	clips = clip_boundaries(seg_start_data['start'],\
							seg_start_data['end'],\
							clip_size)

	boundaries = []
	streams = []
	relevant_clips = set()

	if threads == None:
		pre_parsed = [_file_get(file) for file in _all_files(output)]
	else:
		pre_parsed = threads.map(_file_get, _all_files(output))

	for header_data, video in pre_parsed:

		if condition(header_data):
			pstart, pend = find_clip_boundaries((header_data['start'], \
											     header_data['end']), \
												 clips)

			for rel_clip in range(pstart, pend+1):
					
				cH = cut_header(header_data, clips[rel_clip][0], clips[rel_clip][1])

				if condition(cH):
					relevant_clips.add(rel_clip)

			boundaries.append((header_data['start'],header_data['end']))
			
		streams.append(VideoStream(video))

	#sort the list
	relevant_clips = sorted(list(relevant_clips))

	return [materialize_clip(clips[i], boundaries, streams) for i in relevant_clips]