import cv2
from deeplens.struct import *
from deeplens.dataflow.xform import *
from deeplens.dataflow.map import *
import numpy as np

def image_grid(img, gridsize):
	patches = []
	xl,yl = img.shape

	for x in range(gridsize,xl,gridsize):
		for y in range(gridsize,yl,gridsize):
			patches.append(img[x-gridsize:x,y-gridsize:y])

	return patches

def normalize(vec):
	return np.squeeze(vec)/np.linalg.norm(vec)


def match_frames(img1, img2, gridsize, \
				stride, padding, locations):

	#hog = cv2.HOGDescriptor()
	img1_patches = image_grid(img1, gridsize)
	img2_patches = image_grid(img2, gridsize)

	hog = cv2.HOGDescriptor()

	winStride = stride
	padding = padding

	img1_hog = [normalize(hog.compute(patch, winStride, padding, locations)) for patch in img1_patches]
	img2_hog = [normalize(hog.compute(patch, winStride, padding, locations)) for patch in img2_patches]

	tot = 0
	cnt = 0
	for i in img1_hog:
		for j in img2_hog:
			tot += np.dot(i,j)
			cnt += 1

	return tot/cnt


def is_video_match(input1, input2, thresh=50, sampling=1, gridsize=400, stride=(8,8), padding=(8,8),locations=[(20,20)]):
	vstream1 = VideoStream(input1)[Sample(sampling)][Grayscale()]
	vstream2 = VideoStream(input2)[Sample(sampling)][Grayscale()]

	for frame1 in vstream1:
		for frame2 in vstream2:
			res = match_frames(frame1['data'], frame2['data'], gridsize, stride, padding, locations)

			print(res, input1, input2)

			if res >= thresh:
				return True

	return False
		

	
	


