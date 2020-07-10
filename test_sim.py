import sys
from experiments.environ import *

from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.optimizer.deeplens import DeepLensOptimizer

from deeplens.struct import *
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *
from deeplens.utils.testing_utils import *
from deeplens.dataflow.agg import *
from deeplens.tracking.contour import *
from deeplens.tracking.event import *
from deeplens.core import *
from deeplens.simple_manager.manager import *
import os

import cv2
import numpy as np
from scipy import stats


from deeplens.dataflow.xform import *
from deeplens.utils.ui import play, overlay
from deeplens.constants import *

import matplotlib.pyplot as plt


left = Box(1500, 1600, 1750, 1800)
middle = Box(1750, 1600, 2000, 1800)
right = Box(2000, 1600, 2250, 1800)

v = VideoStream('/Users/sanjaykrishnan/Downloads/brooklyn.mp4')
v = v[GoodKeyPoints()][ActivityMetric('left', left)][
        ActivityMetric('middle', middle)][ActivityMetric('right', right)][
        Filter('left', [1,1,1], 3, delay=10)][
        Filter('middle', [1,1,1], 3, delay=10)][
        Filter('right', [1,1,1], 3, delay=10)]

count = {'left':0, 'middle':0 ,'right':0}
for p in v:

    count['left'] += p['left']
    count['middle'] += p['middle']
    count['right'] += p['right']

    print(count, p['frame'])


    #cv2.imshow('Player',cv2.resize(p['data'], (0,0), fx=0.25,fy=0.25))
    #if cv2.waitKey(1) & 0xFF == ord('q'):
    #    exit()


"""
FILE = 'crash1.mp4'
GT = '/Users/sanjaykrishnan/Downloads/LV_v1/VIDS/GT/g' + FILE
VID = '/Users/sanjaykrishnan/Downloads/LV_v1/VIDS/Test/' + FILE

v = VideoStream(GT)
pipeline = v[KeyPoints(blur=1)]

kps = []
for p in pipeline:
    image = p['data']
    kps.append(len(p['bounding_boxes']))
    image = overlay(image, p['bounding_boxes'])

    #print(p['bounding_boxes'])
    #hit q to exit
    #cv2.imshow('Player',image)
    #if cv2.waitKey(1) & 0xFF == ord('q'):
    #    exit()

v = VideoStream(VID)
pipeline = v[MotionVectors()][Speed()]

kps1 = []
tindex = []
for p in pipeline:
    image = p['data']
    #print(image.shape)
    #print(#)

    #mvs = [l for l,b in p['bounding_boxes']]
    #kps1.extend(mvs)

    if len(p['motion_vectors']) > 0:
        v = np.nanmean(p['motion_vectors'])
        kps1.append(v)
    else:
        kps1.append(np.nan)
    #tindex.extend([p['frame']] * len(mvs))

    #print(kps1)

    #kps1.append(len(p['bounding_boxes']))
    #image = overlay(image, p['bounding_boxes'])

from deeplens.tracking.ts import *

import matplotlib.pyplot as plt

plt.subplot(2,1,1)
plt.plot(moving_average(kps1,100))
plt.subplot(2,1,2)
plt.plot(kps)
plt.show()

print(thresh_finder(kps1,100,100))
#print(change_finder(kps,100,20))
"""
