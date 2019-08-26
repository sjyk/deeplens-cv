"""file.py contains the basic file I/O methods in the deeplens storage manager. It
   provies the basic routines for compressing and formatting video files on disk.
   
   The basic file format is as follows:
       * headers (labels/bounding boxes of objects in a particular frame), compressed in gzip or bzip2
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

def get_rnd_strng():
	return ''.join(random.choice(string.ascii_lowercase) for i in range(10))

def add_ext(name, ext, seq=-1):

	#add period if not
	if '.' not in ext[0]:
		ext = '.' + ext 

	if seq != -1:
		ext = str(seq) + ext 
	
	return name + ext


def write_block(data, path):
	"""Writes a dictionary of serializable data to a file.

	Input: data- dictionary
		   path- directory path to write to
	"""

	#generate a random temp file
	r_name = get_rnd_strng()
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


def build_fmt_file(header_data, \
					video, \
					path, \
					output, \
					header_cmp,\
					meta_cmp,
					header_name=None):
	"""Helper method to write the archive file with the headers and the data
	"""

	file = write_block(header_data, path)

	if header_name == None:
		header_name = get_rnd_strng()

	header = stack_block([file],
						 add_ext(os.path.join(path, header_name), '.head'), 
						 compression=header_cmp)	

	return stack_block([video, header], output, compression=meta_cmp)

