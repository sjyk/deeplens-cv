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

#v = VideoStream('/Users/sanjayk/Downloads/traffic-001.mp4', limit=2000)

FILE = 'crash0.mp4'
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
pipeline = v[MotionVectors()][Direction()]

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

print(change_finder(kps1,100,20))
print(change_finder(kps,100,20))
