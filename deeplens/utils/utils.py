"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

utils.py defines some utilities that can be used for debugging and manipulating
image streams.
"""

import cv2
import numpy as np
import itertools

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
def overlay(frame, bbs):
	ff = np.copy(frame)

	for label, bb in bbs:
		cv2.rectangle(ff, (bb[0],bb[2]), (bb[1],bb[3]),(0,255,0), 2)
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
