
"""Test defines a number of test scenarios that are informative of how the API works
"""

from deeplens.struct import *
from deeplens.utils.ui import *
from deeplens.dataflow.map import *
from deeplens.dataflow.agg import *
from deeplens.tracking.contour import *
from deeplens.tracking.event import *

import cv2
import numpy as np


#count the number of cars in the left lane
v = VideoStream('/Users/sanjaykrishnan/Downloads/TownCentreXVID.avi', limit=1000)
region = Box(600,100,650,250)

pipeline = v[KeyPoints(blur=3)][ActivityMetric('one', region)][Filter('one', [-0.5,1,-0.5], 0.25, delay=40)]

print('Total', count(pipeline, ['one'], stats=True))


#count the number of cars in the left lane
v = VideoStream('/Users/sanjaykrishnan/Downloads/TownCentreXVID.avi', limit=1000)
region = Box(600,100,650,250)
pipeline = v[Crop(600,100,650,250)][KeyPoints(blur=3)][ActivityMetric('one', region)][Filter('one', [-0.5,1,-0.5], 0.25, delay=40)]

print('Total', count(pipeline, ['one'], stats=True))