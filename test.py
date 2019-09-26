from dlcv.object_detection.detect import *
from dlcv.struct import *
from dlcv.utils import *
from dlcv.dataflow.map import *
from dlcv.tracking.contour import *
from dlcv.tracking.event import *

import cv2
import numpy as np

v = VideoStream('/Users/sanjaykrishnan/Downloads/tcam.mp4', limit=1000)

"""
s = TensorFlowObjectDetect('/Users/sanjaykrishnan/Downloads/faster_rcnn_resnet50_coco_2018_01_28/', \
		   'resources/models/ssd_mobilenet_v1_coco_2017_11_17/mscoco_label_map.pbtxt',
		   90,
		   0.05,
		   ['car'], {'buffer_size': 10})
"""


#prev = None
img_set = []
#image_match(k['data'], j['data'], hess_thresh=150)
#play(v[Grayscale()])

counter = 0
region = Box(200,550,350,750)
for i in v[KeyPoints()][Metric(countIn(region),'one')][Filter('one', 'one_dir', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]:
	
	print('Cars: ', counter)
	counter += i['one_dir']
	#cv2.imshow("Edges", overlay(i['data'], [('region',region.serialize())]))

	#if cv2.waitKey(1) & 0xFF == ord('q'):
	#	break


	#if len(k['bounding_boxes']) > 0:
	#	for label, bb in k['bounding_boxes']:
	#		img_set.append(bb_crop(k['data'], bb))

		#show(img)

		#print(dominant_color(img))

"""
for i in img_set:
	for j in img_set:
		print(image_match(i, j, hess_thresh=50,accept=0.25))
"""


		#print(dominant_color(bb_crop(img, k['bounding_boxes'][0][1])))

#print(k)


