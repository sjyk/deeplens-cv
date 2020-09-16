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
import os
import numpy as np
import psutil
from multiprocessing import Process, Value, Manager
from statistics import median

def getResourceUsage(done, mlist):
    cpu_usage = []
    mlist.append(cpu_usage)
    ram_usage = []
    mlist.append(ram_usage)
    while done.value:
        cpu_usage.append(psutil.cpu_percent())
        ram_usage.append(psutil.virtual_memory().percent)
        mlist[0] = cpu_usage
        mlist[1] = ram_usage
        time.sleep(1)
    

def diagnostic(video_path, size):
    os.system("sudo sync; echo 1 | sudo tee /proc/sys/vm/drop_caches >/dev/null")
    file_size = None
    time_storage = None
    usage_storage = None
    time_retreive = None
    usage_retrieve = None

    FILENAME = video_path #the video file that you want to load
    LIMIT = 100

    vstream = VideoStream(FILENAME, limit=LIMIT) #limit is the max number of frames

    vstream = vstream[Crop(0, 0, size[0], size[1])]

    manager = Manager()
    mlist = manager.list()
    done = Value('i', 1)
    resource = Process(target=getResourceUsage, args=(done, mlist))
    resource.start()

    t0 = time.time()

    # /dev/shm/
    cache = persist(vstream, 'cache.npz') #how big the size of the stored raw video is

    done.value -= 1
    resource.join()
    usage_storage = mlist

    manager = Manager()
    mlist = manager.list()
    done = Value('i', 1)
    resource = Process(target=getResourceUsage, args=(done, mlist))
    resource.start()

    t1 = time.time()

    vstream = RawVideoStream('cache.npz', shape=(LIMIT,size[1],size[0],3)) #retrieving the data (have to provide dimensions (num frames, w, h, channels)
    #do something
    for v in vstream:
        np.copy(v['data'], order='F')
        pass

    t2 = time.time()

    done.value -= 1
    resource.join()
    usage_retrieve = mlist

    time_storage = t1 - t0
    time_retreive = t2 - t1

    file_size = os.path.getsize('cache.npz')

    return (file_size, time_storage, time_retreive, median(usage_storage), max(usage_storage), median(usage_retrieve), max(usage_retrieve))

# Test
#print(diagnostic('../tcam.mp4', (1080, 1080)))