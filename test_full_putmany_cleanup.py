import os
import shutil

import pandas as pd
from deeplens.dataflow.agg import count, counts
from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_manager import FullStorageManager
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.constants import *
from deeplens.simple_manager.manager import SimpleStorageManager
from deeplens.struct import VideoStream
from deeplens.tracking.contour import KeyPoints
from experiments.environ import logrecord
from timeit import default_timer as timer


#loads directly from the mp4 file
def runNaive(src, cleanUp = False):
    if cleanUp:
        if os.path.exists('./videos_naive'):
            shutil.rmtree('./videos_naive')

    c = VideoStream(src)
    pipelines = c[KeyPoints()]
    result = count(pipelines, ['one'], stats=True)

    logrecord('naive',({'file': src}), 'get', str(result), 's')


def runSimple(src, cleanUp = False):
    if cleanUp:
        if os.path.exists('./videos_simple'):
            shutil.rmtree('./videos_simple')

    manager = SimpleStorageManager('videos_simple')
    now = timer()
    manager.put(src, os.path.basename(src), args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 30})
    put_time = timer() - now
    logrecord('simple', ({'file': src}), 'put', str({'elapsed': put_time}), 's')

    clips = manager.get(os.path.basename(src), lambda f: True)
    pipelines = []
    for c in clips:
        pipelines.append(c[KeyPoints()])
    result = counts(pipelines, ['one'], stats=True)
    logrecord('simple', ({'file': src}), 'get', str(result), 's')


def runFull(src, cleanUp = False):
    if cleanUp:
        if os.path.exists('./videos_full'):
            shutil.rmtree('./videos_full')

    manager = FullStorageManager(None, CropSplitter(), 'videos_full')
    now = timer()
    manager.put(src, os.path.basename(src), parallel = True, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 30, 'num_processes': os.cpu_count()})
    put_time = timer() - now
    logrecord('full', ({'file': src}), 'put', str({'elapsed': put_time}), 's')

    # Don't call get() for now
    # clips = manager.get(os.path.basename(src), Condition())
    # pipelines = []
    # for c in clips:
    #     pipelines.append(c[KeyPoints()])
    # result = counts(pipelines, ['one'], stats=True)
    # logrecord('full', ({'file': src}), 'get', str(result), 's')

def runFullSequential(src, cleanUp = False):
    if cleanUp:
        if os.path.exists('./videos_full'):
            shutil.rmtree('./videos_full')

    manager = FullStorageManager(None, CropSplitter(), 'videos_full')
    now = timer()
    manager.put(src, os.path.basename(src), parallel = False, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 30, 'num_processes': os.cpu_count()})
    put_time = timer() - now
    logrecord('full', ({'file': src}), 'put', str({'elapsed': put_time}), 's')

    clips = manager.get(os.path.basename(src), Condition())
    pipelines = []
    for c in clips:
        pipelines.append(c[KeyPoints()])
    result = counts(pipelines, ['one'], stats=True)
    logrecord('full', ({'file': src}), 'get', str(result), 's')

def runFullPutMany(src_list, cleanUp = False):
    if cleanUp:
        if os.path.exists('./videos_full'):
            shutil.rmtree('./videos_full')

    manager = FullStorageManager(None, CropSplitter(), 'videos_full')
    now = timer()
    targets = [os.path.basename(src) for src in src_list]
    logs = manager.put_many(src_list, targets, log = True, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 30, 'num_processes': os.cpu_count()})
    put_time = timer() - now
    logrecord('full', ({'file': src_list}), 'put', str({'elapsed': put_time}), 's')
    for i, log in enumerate(logs):
        logrecord('fullMany', i, 'put', str({'elapsed': log}), 's')

    # Don't call get() for now
    # for src in src_list:
    #     clips = manager.get(os.path.basename(src), Condition())
    #     pipelines = []
    #     for c in clips:
    #         pipelines.append(c[KeyPoints()])
    #     result = counts(pipelines, ['one'], stats=True)
    #     logrecord('full', ({'file': src}), 'get', str(result), 's')


df = pd.read_csv('./deeplens/media/train/bbox_sizes.csv', sep=',',
                 dtype={'youtube_id': str})
youtube_ids=df['youtube_id']
youtube_ids2=list(dict.fromkeys(youtube_ids))


total_start = timer()
runFullPutMany(["./deeplens/media/train/"+item+".mp4" for item in youtube_ids2], cleanUp=True)
print("Total time for full with parallelism across videos (cleanUp = True):", timer() - total_start)
