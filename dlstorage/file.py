"""This module contains the basic file I/O methods
"""

import pickle
import string
import os 
import tarfile
import random
import cv2

#compression constants
GZ,BZ2,RAW = 'w:gz','w:bz2','w'

#encoding constants
XVID, DIVX, H264, MP4V = 'XVID', 'DIVX', 'X264', 'FMP4'

def _get_rnd_strng():
	return ''.join(random.choice(string.ascii_lowercase) for i in range(10))


def write_block(data, path):
	"""Writes a dictionary of serializable data to a file.

	Input: data- dictionary
		   path- directory path to write to
	"""

	#generate a random temp file
	r_name = _get_rnd_strng()
	file_name = os.path.join(path, r_name)

	#open the file
	f = open(file_name, 'wb')

	pickle.dump(data, f)

	f.close()
	return file_name


def read_block(file):
	"""Reads a block from a specified data path.

	Input: file- a block to read
	"""
	f = open(file, 'rb')
	return pickle.load(f)


def stack_block(files, sfile, compression=RAW):
	"""Given a list of files builds a contiguous block

	Input: files- a collection of files
		   sfile- stacked file name to create
		   compression- a meta compression scheme over the blocks
	"""

	tf = tarfile.open(sfile, mode=compression)
	
	for file in files:
		tf.add(file, arcname=os.path.basename(file))

	tf.close()

	return sfile


def unstack_block(sfile, path):
	"""Given a stacked file returns pointers to all of the constituent
	   extracted and decompressed files.

	   Input: sfile- stacked file name to extract
	   		  path- directory to extract all to
	"""
	tf = tarfile.open(sfile)
	tf.extractall(path)
	return [os.path.join(path, n) for n in tf.getnames()]


def _build_fmt_file(header_data, \
					video, \
					path, \
					output, \
					header_cmp,\
					meta_cmp):

	file = write_block(header_data, scratch)
	r_name = _get_rnd_strng()
	header = stack_block([file], os.path.join(path, r_name)+'.head', compression=header_cmp)	
	return stack_block([video, header], output, compression=meta_cmp)



def write_video(vstream, \
				output, \
				encoding, \
				scratch = '/tmp/', \
				frame_rate=30.0, \
				header_cmp=RAW):
	"""Writes a video to disk from a stream
	"""
	# Define the codec and create VideoWriter object
	fourcc = cv2.VideoWriter_fourcc(*encoding)
	start = True
	tags = []

	#tmp file for the video
	r_name = _get_rnd_strng()
	file_name = os.path.join(scratch, r_name) +'.avi'
	
	for frame in vstream:

		if start:
			out = cv2.VideoWriter(file_name,fourcc, frame_rate, (vstream.width, vstream.height),True)
			start = False

		out.write(frame['data'])
		tags.append(frame['tags'])

	_build_fmt_file(tags, file_name, scratch, output, header_cmp, RAW)
		
	return final



def write_video_clips(vstream, \
						output, \
						encoding, \
						clip_size,
						scratch = '/tmp/', \
						frame_rate=30.0, \
						header_cmp=RAW):
	"""Writes a video to disk from a stream
	"""

	# Define the codec and create VideoWriter object
	fourcc = cv2.VideoWriter_fourcc(*encoding)
	counter = 0
	seq = 0
	tags = []

	#tmp file for the video
	r_name = _get_rnd_strng()
	
	for frame in vstream:

		if counter == 0:
			file_name = os.path.join(scratch, r_name) +'.' + str(seq) +'.avi'
			out = cv2.VideoWriter(file_name,fourcc, frame_rate, (vstream.width, vstream.height),True)

		out.write(frame['data'])
		tags.append(frame['tags'])

		if counter == clip_size:
			_build_fmt_file(tags, file_name, scratch, output + '.'+str(seq), header_cmp, RAW)

			counter = 0
			seq += 1
			tags = []


		counter += 1
		
	return final

