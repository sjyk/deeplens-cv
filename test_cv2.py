import os
import cv2
import sys
from timeit import default_timer as timer

filename = sys.argv[1]

now = timer()
cap = cv2.VideoCapture(filename)

fourcc = cv2.VideoWriter_fourcc(*'XVID')
width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

name = filename + '_copy.avi'
frame_rate = 30
writer = cv2.VideoWriter(name, fourcc, frame_rate, (width, height))
while True:
    ret, frame = cap.read()
    if not ret:
        break
    writer.write(frame)

print('finished writing in', timer() - now)
