import cv2
from deeplens.struct import *
from deeplens.dataflow.xform import *
from deeplens.dataflow.map import *
import numpy as np

DEFAULT_FRAME_RATE = 30.0

def avg_change(input, blur):
	vstream = VideoStream(input, limit=100)
	prev = None
	change_array = []

	for frame in vstream[Grayscale()][Blur(blur)]:

		if not (prev is None):
			frameDelta = cv2.absdiff(frame['data'],prev)
			change_array.append(np.mean(frameDelta))

		prev = frame['data']


	return np.mean(change_array)


#takes in a file and breaks it up into discrete shots (returns a list of new files)
def shot_segmentation(input, blur=11, threshold=50, frame_rate=DEFAULT_FRAME_RATE, skip=1, encoding='XVID'):

	# Define the codec and create VideoWriter object
	counter = 0
	seq = 0

	output_files = []

	#gather video statistics
	#mean = avg_change(input,blur)
	#print(mean, std)

	vstream = VideoStream(input)
	prev = None

	for frame in vstream:

		if counter == 0:
			fourcc = cv2.VideoWriter_fourcc(*encoding)

			file_name = input + '.' + str(seq) + '.avi'

			out = cv2.VideoWriter(file_name,
								  fourcc, 
								  frame_rate, 
								  (vstream.width, vstream.height),
								  True)

		orig = frame['data'].copy()

		img = cv2.cvtColor(frame['data'], cv2.COLOR_BGR2GRAY)
		img = cv2.GaussianBlur(img, (blur, blur), 0)

		if not (prev is None) and counter % skip == 0:
			frameDelta = cv2.absdiff(img,prev)

			if np.mean(frameDelta) > threshold:
				
				output_files.append(((frame['frame']-counter,frame['frame']), file_name))

				counter = 0
				seq += 1
				out.release()
				prev = img
				continue

		out.write(orig)
		prev = img
		counter += 1

	return output_files
