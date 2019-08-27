"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

file.py contains the basic file I/O methods in the deeplens storage manager. It
provies the basic routines for compressing and formatting video files on disk.
   
The basic file format is as follows:
* headers (labels/bounding boxes of objects in a particular frame), compressed 
in gzip or bzip2
* video (encoded in a supported encoding format)
"""

import pickle
import string
import os 
import tarfile
import random
import cv2

#import all of the constants
from dlstorage.constants import *


def get_rnd_strng(size=10):
	"""get_rnd_strng() generates a random string for creating temp files.

	Args:
		size (int) - Size of the string. (Default 10).
	"""
	return ''.join(random.choice(string.ascii_lowercase) for i in range(size))


def add_ext(name, ext, seq=-1):
	"""add_ext constructs file names with a name, sequence number, and 
	extension.

	Args:
		name (string) - the filename
		ext (string) - the file extension
		seq (int) - the number in a sequence with other files 
					(Default -1) means that it is a standalone 
					file
	"""

	#add period if not
	if '.' not in ext[0]:
		ext = '.' + ext 

	if seq != -1:
		ext = str(seq) + ext 
	
	return name + ext



def write_block(data, path, name=None):
	"""Writes a dictionary of serializable data to a file.

	Args:  
		data (dict) - dictionary
		path (string)- a temp directory path to write to
		name (string) - optional write to a specific file
	"""

	#generate a random temp file
	if name == None:
		r_name = get_rnd_strng()
		
	else:
		r_name = name

	file_name = os.path.join(path, r_name)

	#open the file
	f = open(file_name, 'wb')

	pickle.dump(data, f)

	f.close()
	return file_name


def read_block(file):
	"""Reads a block from a specified data path.

	Args: 
		file (string) - a file name of a block to read
	"""
	f = open(file, 'rb')
	return pickle.load(f)


def stack_block(files, sfile, compression=RAW):
	"""Given a list of files builds a contiguous block

	Args:  
		files- a collection of files
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

	   Args: 
	   		sfile- stacked file name to extract
	   		path- directory to extract all to
	"""
	tf = tarfile.open(sfile)
	tf.extractall(path)
	return [os.path.join(path, n) for n in tf.getnames()]


"""This is the main method that interfaces with the video storage system, 
it constructs an archive file with header information as well as the video
information.
"""
def build_fmt_file(header_data, \
					video, \
					path, \
					output, \
					header_cmp,\
					meta_cmp,
					header_name=None):
	"""Helper method to write the archive file with the headers and the data.

	Args:
		header_data - header data in dict form that will describe what is 
					 in a video clip.
		video - video file on disk
		path  - scratch or temp space to store the data
		output - output file name
		header_cmp - how to compress the header
		meta_cmp - how to compress the whole archive (header + video)
		header_name - how to name the header
	"""

	file = write_block(header_data, path)

	if header_name == None:
		header_name = get_rnd_strng()

	header = stack_block([file],
						 add_ext(os.path.join(path, header_name), '.head'), 
						 compression=header_cmp)	

	return stack_block([video, header], output, compression=meta_cmp)

