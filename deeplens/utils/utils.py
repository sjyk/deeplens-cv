"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

utils.py defines some utilities that can be used for debugging and manipulating
image streams.
"""

import cv2
import numpy as np
import itertools
import copy
from deeplens.utils.box import Box
import os
import json
import random
import string

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

#plays video stream through the system player
def play(vstream):
	for frame in vstream:
		cv2.imshow('Player',frame['data'])
		if cv2.waitKey(3) & 0xFF == ord('q'):
			break

#shows a single frame
def show(frame):
	cv2.imshow('Debug',frame)
	cv2.waitKey(0)

#overlays a bounding box with labels over a frame
def overlay(frame, bbs, labelp=True):
	ff = np.copy(frame)

	for label, bb in bbs:
		cv2.rectangle(ff, (bb[0],bb[2]), (bb[1],bb[3]),(0,255,0), 2)
		
		if labelp:
			cv2.putText(ff, label, (bb[0],bb[2]), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0, 255, 0), lineType=cv2.LINE_AA) 

	return ff

#crop and replace primitives
def bb_crop(frame, box):
	ff = np.copy(frame)
	return ff[box.y0:box.y1,box.x0:box.x1]

def bb_replace(frame1, box, frame2):
	ff = np.copy(frame1)
	ff[box.y0:box.y1,box.x0:box.x1] = frame2
	return ff

def mask(frame, mask):
    """Masks the content of a frame of the video
    """
    frame = copy.deepcopy(frame)
    frame = frame[mask]
    return frame

def crop_box(frame, box):
    """ Crops a frame of the video to a box 
    given by the input arguments
    """
    frame = copy.deepcopy(frame)
    frame = frame[box.y0:box.y1, box.x0:box.x1]
    return frame


def reverse_crop(frame, crops):
    """Masks the content of a frame of the video
    to remove all crops
    """
    frame = copy.deepcopy(frame)
    for crop in crops:
        box = crop['bb']
        frame[box.y0:box.y1, box.x0:box.x1] = 0
    return frame

#matches frames against each other
def image_match(im1, im2, hess_thresh=150, dist_threshold=1000, accept=0.75):
	brisk = cv2.BRISK_create(thresh=hess_thresh)
	(kps1, descs1) = brisk.detectAndCompute(im1, None)
	(kps2, descs2) = brisk.detectAndCompute(im2, None)

	match_cnt = 0
	for i,k in enumerate(kps1):
		best_match = None

		for j,k in enumerate(kps2):

			distance = np.linalg.norm(descs2[j]-descs1[i])

			if distance < dist_threshold:
				if best_match == None:
					best_match = (j, distance)
				else:
					best_match = (j,min(best_match[1], distance))

		match_cnt += (best_match != None)

	if len(kps1) == 0:
		return False
	return (match_cnt/len(kps1) >= accept)
	
def labels_to_intervals(labels_list):
    """
    labels_to_intervals() converts list of labels of each frame into set of time intervals where a tag occurs

    Args:
        labels_list: list of labels of each frame
        e.g. [{'person'}, {'person'}, {'person'}, {'surfboard', 'person'}]

    Returns:
        tags - set of time intervals where a tag occurs:
            { (label, start, end) }, a video from time 0 (inclusive) to time T (exclusive)
            e.g. {('cat', 3, 9), ('dog', 5, 8), ('people', 0, 6)}
            e.g. {('cat', 0, 1), ('cat', 2, 4), ('cat', 6, 8), ('dog', 0, 3),
                  ('dog', 6, 8), ('people', 0, 2), ('people', 4, 6)}

    """

    labels_dict = dict()
    for frame, labels in enumerate(labels_list):
        for label in labels:
            if label in labels_dict:
                labels_dict[label].add(frame)
            else:
                labels_dict[label] = {frame}

    output = set()
    for key, value in labels_dict.items():
        frame_list = sorted(value)
        for interval in [(t[0][1], t[-1][1]) for t in
                         (tuple(g[1]) for g in itertools.groupby(enumerate(frame_list), lambda x: x[0]-x[1]))]:
            output.add((key, interval[0], interval[1]+1))
    return output

def parallel_log_reduce(logs, start_time):
    times = []
    for log in logs:
        with open(log, 'r') as f:
            time = json.load(f)
            time['end_time'] = time['end_time'] - start_time
            times.append(time['end_time'])
    times.sort()
    return times

def paralleL_log_delete(logs):
    for log in logs:
        os.remove(log)

class Serializer(json.JSONEncoder):
	def default(self, obj):
		return obj.serialize()