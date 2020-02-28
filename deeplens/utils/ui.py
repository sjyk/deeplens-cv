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
def overlay(frame, bbs, labelp=True, thickness=10):
	ff = np.copy(frame)

	for label, bb in bbs:
		cv2.rectangle(ff, (bb[0],bb[1]), (bb[2],bb[3]),(0,255,0), thickness)
		
		if labelp:
			cv2.putText(ff, label, (bb[0],bb[1]), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0, 255, 0), lineType=cv2.LINE_AA) 

	return ff

#crop and replace primitives
def bb_crop(frame, box):
	ff = np.copy(frame)
	return ff[box.y0:box.y1,box.x0:box.x1]

def bb_replace(frame1, box, frame2):
	ff = np.copy(frame1)
	ff[box.y0:box.y1,box.x0:box.x1] = frame2
	return ff