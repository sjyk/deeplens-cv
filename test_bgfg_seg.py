
"""Test defines a number of test scenarios that are informative of how the API works
"""

from deeplens.struct import *
from deeplens.utils.ui import *
from deeplens.dataflow.map import *
from deeplens.tracking.background import *

import cv2
import numpy as np


#count the number of cars in the left lane
v = VideoStream('tcam.mp4', limit=1000)
f = FixedCameraBGFGSegmenter()
#play(v)
fg = f.segment(v)
#print(fg)

play(v[Crop(*fg['bb'])])
