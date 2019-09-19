from dlstorage.stream import *
import cv2
import numpy as np


img_stream = [cv2.imread('office.png')]*10
i = IteratorVideoStream(img_stream)

for j in i:
	print(j)

