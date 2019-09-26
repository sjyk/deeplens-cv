import cv2
import numpy as np

def play(vstream):
	for frame in vstream:
		cv2.imshow('Player',frame['data'])
		if cv2.waitKey(3) & 0xFF == ord('q'):
			break

def show(frame):
	cv2.imshow('Debug',frame)
	cv2.waitKey(0)

def overlay(frame, bbs):
	ff = np.copy(frame)

	for label, bb in bbs:
		cv2.rectangle(ff, (bb[0],bb[2]), (bb[1],bb[3]),(0,255,0), 2)
		cv2.putText(ff, label, (bb[0],bb[2]), cv2.FONT_HERSHEY_SIMPLEX, 1.0, (0, 255, 0), lineType=cv2.LINE_AA) 

	return ff

def bb_crop(frame, box):
	ff = np.copy(frame)
	return ff[box.y0:box.y1,box.x0:box.x1]


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
	

