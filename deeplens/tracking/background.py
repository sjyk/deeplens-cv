"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

background.py defines some background-foreground separation routines that
are useful for processing fixed camera videos.
"""

import cv2
import numpy as np

#fixed camera bg segmentation
#finds "moving" pixels
class FixedCameraBGFGSegmenter(object):

	def __init__(self, movement_threshold=25, #bigger means that movements needs to be more significant
				 blur=21, #reduces noise
				 movement_prob=0.05): #each pixel has a probability of movement.

		self.movement_threshold = movement_threshold
		self.blur = blur
		self.movement_prob = movement_prob


	#returns a bounding box around the foreground.
	def segment(self, vstream):

		dynamic_mask = None
		prev = None
		count = 0

		for frame in vstream:
			img = frame['data']

			gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
			blurred = cv2.GaussianBlur(gray, (self.blur, self.blur), 0)

			if not (prev is None):

				frameDelta = cv2.absdiff(blurred, prev)
				thresh = cv2.threshold(frameDelta, self.movement_threshold, 255, cv2.THRESH_BINARY)[1]

				if dynamic_mask is None:
					dynamic_mask = thresh.astype(np.float32)
				else:
					dynamic_mask += thresh.astype(np.float32)/255
	

			prev = blurred
			count += 1	

		cthresh = count * self.movement_prob

		y0 =  np.min(np.argwhere(dynamic_mask > cthresh), axis=0)[0]
		x0 =  np.min(np.argwhere(dynamic_mask > cthresh), axis=0)[1]
		y1 =  np.max(np.argwhere(dynamic_mask > cthresh), axis=0)[0]
		x1 =  np.max(np.argwhere(dynamic_mask > cthresh), axis=0)[1]

		#flipped axis in crop
		
		return (x0, y0, x1, y1)