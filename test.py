#from dlcv.object_detection.detect import *
from dlcv.struct import *
from dlcv.utils import *
from dlcv.dataflow.map import *
from dlcv.dataflow.agg import *
from dlcv.dataflow.validation import *
from dlcv.tracking.contour import *
from dlcv.tracking.event import *

import cv2
import numpy as np

v = VideoStream('/Users/sanjaykrishnan/Downloads/tcam.mp4', limit=1000)

region = Box(200,550,350,750)
pipeline = v[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]

#print('Left', count(pipeline, 'one', stats=True))

print(countable(pipeline.logical_plan(), 'one'))
#print(build(pipeline.logical_plan()))


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


"""
region = Box(200,550,350,750)
pipeline = v[KeyPoints()][Metric(countIn(region),'one')][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
print('Left', count(pipeline, 'one', stats=True))
"""

"""
counter = 0
region = Box(200,550,350,750)
k = KeyPoints()
k.setCrop(Box(100,450,450,950))
print('Left', count(v[k][Metric(countIn(region),'one')][Filter('one', 'one_dir', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)], 'one_dir', stats=True))
"""

"""
region = Box(500,550,650,750)
pipeline = v[KeyPoints()][Metric(countIn(region),'one')][Filter('one', 'one_dir', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
print('Right', count(pipeline, 'one_dir', stats=True))
"""


"""
counter = 0
region = Box(200,550,350,750)
for i in v[KeyPoints()][Metric(countIn(region),'one')][Filter('one', 'one_dir', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]:
	
	print('Cars: ', counter)
	counter += i['one_dir']
	cv2.imshow("Edges", overlay(i['data'], [('region',region.serialize())]))

	if cv2.waitKey(1) & 0xFF == ord('q'):
		break
"""
