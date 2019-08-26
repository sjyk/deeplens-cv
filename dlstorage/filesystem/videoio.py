"""This module handles reading and writing videos
"""

from dlstorage.filesystem.file import *
from dlstorage.constants import *
from dlstorage.stream import *

import cv2
import os


def write_video(vstream, \
				output, \
				encoding, \
				header,
				scratch= DEFAULT_TEMP, \
				frame_rate=DEFAULT_FRAME_RATE, \
				header_cmp=RAW):
	"""Writes a video to disk from a stream
	"""

	# Define the codec and create VideoWriter object
	fourcc = cv2.VideoWriter_fourcc(*encoding)
	start = True
	tags = []

	#tmp file for the video
	r_name = get_rnd_strng()
	seg_name = os.path.join(scratch, r_name)

	file_name = add_ext(seg_name, AVI)
	
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

	return build_fmt_file(header.getHeader(), \
						   file_name, \
						   scratch, \
						   add_ext(output, '.seq', 0), 
						   header_cmp, \
						   RAW,\
						   seg_name)




def write_video_clips(vstream, \
						output, \
						encoding, \
						header,
						clip_size,
						scratch = DEFAULT_TEMP, \
						frame_rate=DEFAULT_FRAME_RATE, \
						header_cmp=RAW):
	"""Writes a video to disk from a stream in clips of a specified size
	"""

	# Define the codec and create VideoWriter object
	counter = 0
	seq = 0

	output_files = []
	
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
			

		counter += 1


	if counter != 1:
		output_files.append(build_fmt_file(header.getHeader(), \
												file_name, \
												scratch, \
												add_ext(output, '.seq', seq), \
												header_cmp, \
												RAW, 
												seg_name))

		header.reset()
		out.release()
		
	return output_files




def read_if(output, condition, scratch = DEFAULT_TEMP):
	seq = 0
	streams = []

	while True:

		try:
			file = add_ext(output, '.seq', seq) 
			parsed = unstack_block(file, scratch)

			if '.head' in parsed[0]:
				head, video = 0, 1
			else:
				head, video = 1, 0

			header = unstack_block(parsed[head], scratch)
			header_data = read_block(header[0])

			if condition(header_data):
				streams.append(VideoStream(parsed[video]))

			seq += 1

		except FileNotFoundError:
			break

	return streams
