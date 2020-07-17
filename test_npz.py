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

from deeplens.extern.cache import persist

FILENAME = '' #the video file that you want to load
vstream = VideoStream(FILENAME, limit=1000) #limit is the max number of frames

size = persist(vstream, 'cache.npz') #how big the size of the stored raw video is

from deeplens.struct import RawVideoStream

vstream = RawVideoStream('cache.npz', shape=(1000,1080,1920,3)) #retrieving the data (have to provide dimensions (num frames, w, h, channels)

#do something
for v in vstream:
    pass

