import os
import sys
import inspect

# Is there a better way?
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

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

from deeplens.extern.cache import persist
from deeplens.struct import RawVideoStream

sys.path.insert(0,currentdir)

import time

def diagnostic(video_path):
    file_size = None
    time_storage = None
    time_retreive = None

    FILENAME = video_path #the video file that you want to load
    LIMIT = 100

    vstream = VideoStream(FILENAME, limit=LIMIT) #limit is the max number of frames

    t0 = time.time()

    cache = persist(vstream, 'cache.npz') #how big the size of the stored raw video is

    t1 = time.time()

    vstream = RawVideoStream('cache.npz', shape=(LIMIT,1080,1920,3)) #retrieving the data (have to provide dimensions (num frames, w, h, channels)
    #do something
    f = 0
    for v in vstream:
        f += 1
        pass

    t2 = time.time()

    time_storage = t1 - t0
    time_retreive = t2 - t1

    file_size = os.path.getsize('cache.npz')

    return (file_size, time_storage, time_retreive)


# Test
# print(diagnostic('../tcam.mp4'))