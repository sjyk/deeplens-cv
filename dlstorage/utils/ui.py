import cv2
from dlstorage.constants import *

def play(vstream):
	for frame in vstream:
		cv2.imshow('Player',frame['data'])
		if cv2.waitKey(DEFAULT_FRAME_RATE) & 0xFF == ord('q'):
			break