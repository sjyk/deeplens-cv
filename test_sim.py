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

import cv2
import numpy as np


from deeplens.dataflow.xform import *
from deeplens.utils.ui import play, overlay

#v = VideoStream('/Users/sanjayk/Downloads/traffic-001.mp4', limit=2000)
v = VideoStream('/Users/sanjayk/Downloads/panoramic-000-001.mp4', limit=2000)
pipeline = v[SizeMovementDetector()][KeyPointFilter()]

for p in pipeline:
    image = p['data']
    image = overlay(image, p['bounding_boxes'])

    #print(p['bounding_boxes'])
    #hit q to exit
    cv2.imshow('Player',image)

    #print("test")

    if cv2.waitKey(1) & 0xFF == ord('q'):
        exit()