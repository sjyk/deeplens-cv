
"""Test defines a number of test scenarios that are informative of how the API works
"""

from dlcv.struct import *
from dlcv.utils import *
from dlcv.dataflow.map import *
from dlcv.dataflow.agg import *
from dlcv.tracking.contour import *
from dlcv.tracking.event import *

import cv2
import numpy as np
import sys

sys.setrecursionlimit(10000)
if not sys.warnoptions:
    import warnings
    warnings.simplefilter("ignore")

frame_limit = 5000

#count the number of cars in the left lane
print("\nRegular counting: number of cars in the left lane")
v = VideoStream('/Users/brunobarbarioli/Documents/Research/videos/tcam.mp4', limit=frame_limit)
region = Box(200,550,350,750)
pipeline = v[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
print('Left', count(pipeline, ['one'], stats=True))
"""
#print([(p.__class__.__name__, p.serialize()) for p in pipeline.lineage()[1:]])

#count the number of cars in the right lane
print("\nRegular counting: number of cars in the right lane")
v = VideoStream('tcam.mp4', limit=1000)
region = Box(500,550,650,750)
pipeline = v[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
print('Right', count(pipeline, ['one'], stats=True))


#optimize car counting by pushing down a crop()
print("\nOptimized car counting by pushing down a crop: number of cars in the left lane")
v = VideoStream('tcam.mp4', limit=1000)
region = Box(200,550,350,750)

k = KeyPoints()
k.setCrop(Box(100,450,450,950)) #notice it is slightly bigger than the actual region

pipeline = v[k][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
print('Left', count(pipeline, ['one'], stats=True))

"""
#Optimize car counting by  naive sampling
print("\nOptimized car counting by naive sampling: number of cars in the left lane")
v = VideoStream('/Users/brunobarbarioli/Documents/Research/videos/tcam.mp4', limit=frame_limit)
region = Box(200,550,350,750)
pipeline = v[Sample(0.5)][KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
print('Left', count(pipeline, ['one'], stats=True))

#Optimize car counting by  EBGS sampling
print("\nOptimized car counting by EBS sampling: number of cars in the left lane")
v = VideoStream('/Users/brunobarbarioli/Documents/Research/videos/tcam.mp4', limit=frame_limit)
region = Box(200,550,350,750)
pipeline = v[EBGS(0.5, 0.01, 5)][KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
print('Left', count(pipeline, ['one'], stats=True))

#Optimize car counting by random uniform sampling
print("\nOptimized car counting by random uniform sampling: number of cars in the left lane")
v = VideoStream('/Users/brunobarbarioli/Documents/Research/videos/tcam.mp4', limit=frame_limit)
region = Box(200,550,350,750)
pipeline = v[RUS(3)][KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
print('Left', count(pipeline, ['one'], stats=True))

#Optimize car counting by block sampling

