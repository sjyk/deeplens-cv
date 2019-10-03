"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

utils.py defines some utilities that can be used for debugging and manipulating
image streams.
"""

import cv2
import numpy as np

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
	

