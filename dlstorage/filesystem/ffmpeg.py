import os
import subprocess
from dlstorage.utils.clip import *

from multiprocessing import Pool

def get_duration(file):
	ARGS = 'ffprobe -v error -show_entries format=duration -of default=noprint_wrappers=1:nokey=1'.split()
	ARGS.append(file)
	result = subprocess.run(ARGS, stdout=subprocess.PIPE)
	return int(float(result.stdout))
 

def _inner(block):
	start, filename, manager, args, target = block

	block_target = target + '_' + str(block[0])

	if args == None:
		args = manager.DEFAULT_ARGS.copy()
	else:
		args = args.copy()

	args['offset'] = start
	
	manager.doPut(filename, block_target, args=args)

	os.remove(filename)
	

def block_put(file_manager,
			  file,\
			  target, 
			  block_size=60,
			  args=None):
	
	clips = clip_boundaries(0,get_duration(file),block_size)
	files = [(start,filename, file_manager, args, target) \
				for start,filename in blocks(file, clips, block_size)]

	if file_manager.threads == None:
		list(map(_inner, files))
	else:
		Pool(file_manager.threads).map(_inner, files)


def is_target(name, video):
	if not (name + '_') in video:
		return False
	return True


def block_get(file_manager,
			  name, 
			  condition, 
			  clip_size):
	
	rtn = []
	for video in file_manager.list():
		if is_target(name, video):
			rtn += file_manager.doGet(name, condition, clip_size)
	return rtn


def blocks(file,
		   clips,
		   block_size):
	filname, ext = os.path.splitext(file)
	rtn = []

	seq = 0
	for start, end in clips:
		ARGS = 'ffmpeg -y -ss {} -i {} -c copy -t {}'.format(str(start),file, block_size).split()
		new_file = filname + str(seq) + ext
		ARGS.append(new_file)
		rtn.append((start, new_file))
		result = subprocess.run(ARGS, stdout=subprocess.PIPE)
		seq +=1

	return rtn
