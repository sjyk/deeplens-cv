import os
import sys
import inspect
import tfci
import tensorflow.compat.v1 as tf
import gzip

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
import shutil

def getResourceUsage(done, mlist):
    cpu_usage = []
    ram_usage = []
    read_count = []
    write_count = []
    read_bytes = []
    write_bytes = []
    while done.value:
        cpu_usage.append(psutil.cpu_percent())
        ram_usage.append(psutil.virtual_memory().percent)
        disk_tuple = psutil.disk_io_counters()
        read_count.append(disk_tuple[0])
        write_count.append(disk_tuple[1])
        read_bytes.append(disk_tuple[2])
        write_bytes.append(disk_tuple[3])
        time.sleep(1)
    mlist.append(cpu_usage)
    mlist.append(ram_usage)
    mlist.append(read_count)
    mlist.append(write_count)
    mlist.append(read_bytes)
    mlist.append(write_bytes)
    

def diagnostic(video_path, size):
    os.system("sudo sync; echo 1 | sudo tee /proc/sys/vm/drop_caches >/dev/null")
    file_size = None
    time_storage = None
    cpu_storage = None
    ram_storage = None
    read_count_storage = None
    write_count_storage = None
    read_bytes_storage = None
    write_bytes_storage = None
    time_retrieve = None
    cpu_retrieve = None
    ram_retrieve = None
    read_count_retrieve = None
    write_count_retrieve = None
    read_bytes_retrieve = None
    write_bytes_retrieve = None

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

    os.mkdir('cache')
    for i, v in enumerate(vstream):
        f = gzip.GzipFile(f"cache/{i}.tfci", "w")
        np.save(file=f, arr=v)
        f.close()
        #tfci.compress('mbt2018-mean-msssim-8', v['data'], f"cache/{i}.tfci")
        v['data'] = None

    cache = persist(vstream, '/dev/shm/cache.npz')

    done.value -= 1
    resource.join()
    cpu_storage = mlist[0]
    ram_storage = mlist[1]
    read_count_storage = mlist[2]
    write_count_storage = mlist[3]
    read_bytes_storage = mlist[4]
    write_bytes_storage = mlist[5]

    manager = Manager()
    mlist = manager.list()
    done = Value('i', 1)
    resource = Process(target=getResourceUsage, args=(done, mlist))
    resource.start()

    t1 = time.time()

    vstream = RawVideoStream('/dev/shm/cache.npz', shape=(LIMIT,size[1],size[0],3)) #retrieving the data (have to provide dimensions (num frames, w, h, channels)

    for i, v in enumerate(vstream):
        f = gzip.GzipFile(f"cache/{i}.tfci", "w")
        np.load(f)
        f.close()
        v['data'] = tfci.decompress(f"cache/{i}.tfci")

    #do something
    for v in vstream:
        np.copy(v['data'], order='C')
        pass

    t2 = time.time()

    done.value -= 1
    resource.join()
    cpu_retrieve = mlist[0]
    ram_retrieve = mlist[1]
    read_count_retrieve = mlist[2]
    write_count_retrieve = mlist[3]
    read_bytes_retrieve = mlist[4]
    write_bytes_retrieve = mlist[5]

    time_storage = t1 - t0
    time_retrieve = t2 - t1

    total_size = 0
    for root, dirs, files in os.walk("cache"):
        for f in files:
            total_size += os.path.getsize(os.path.join(root, f))
    file_size = total_size

    shutil.rmtree("cache")

    return (file_size, time_storage, time_retrieve, \
            median(cpu_storage), max(cpu_storage), median(ram_storage), max(ram_storage), \
            median(read_count_storage), max(read_count_storage), median(write_count_storage), max(write_count_storage), \
            median(read_bytes_storage), max(read_bytes_storage), median(write_bytes_storage), max(write_bytes_storage), \
            median(cpu_retrieve), max(cpu_retrieve), median(ram_retrieve), max(ram_retrieve), \
            median(read_count_retrieve), max(read_count_retrieve), median(write_count_retrieve), max(write_count_retrieve), \
            median(read_bytes_retrieve), max(read_bytes_retrieve), median(write_bytes_retrieve), max(write_bytes_retrieve), \
            )

# Test
#print(diagnostic('../tcam.mp4', (1080, 1080)))