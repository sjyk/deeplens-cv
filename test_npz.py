import sys


from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.optimizer.deeplens import DeepLensOptimizer

from deeplens.struct import *
from deeplens.utils import *
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *
from deeplens.utils.testing_utils import *
from deeplens.dataflow.agg import *
from deeplens.tracking.contour import *
from deeplens.tracking.event import *
from deeplens.core import *
from deeplens.simple_manager.manager import *

from deeplens.utils.ui import play

import cv2
import numpy as np
import time as t

from deeplens.extern.cache import persist

FILENAME = 'tcam.mp4' #the video file that you want to load
LIMIT = 100
vstream = VideoStream(FILENAME, limit=LIMIT) #limit is the max number of frames

t0 = t.time()

size = persist(vstream, 'cache.npz') #how big the size of the stored raw video is

t1 = t.time()

from deeplens.struct import RawVideoStream

vstream = RawVideoStream('cache.npz', shape=(LIMIT,1080,1920,3)) #retrieving the data (have to provide dimensions (num frames, w, h, channels)


#do something
f = 0
for v in vstream:
    f += 1
    #print(f)
    pass

t2 = t.time()

time_to_storage = t1 - t0
time_to_retrieve = t2 - t1
time_total = t2 - t0

print(f"Storage: {time_to_storage} + Retrieve: {time_to_retrieve} = Total: {time_total}")