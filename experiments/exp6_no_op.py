import os
import shutil
import environ

import pandas as pd
from deeplens.dataflow.agg import count, counts
from deeplens.dataflow.xform import Null
from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_manager import FullStorageManager
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.constants import *
from deeplens.simple_manager.manager import SimpleStorageManager
from deeplens.struct import VideoStream
from deeplens.tracking.contour import KeyPoints
from experiments.environ import logrecord
from timeit import default_timer as timer

df = pd.read_csv('./deeplens/media/train/processed_yt_bb_detection_train.csv', sep=',',
                 dtype={'youtube_id': str})
youtube_ids=df['youtube_id']
youtube_ids2=list(dict.fromkeys(youtube_ids))

def runNaive(src, cleanUp = False):
    if cleanUp:
        if os.path.exists('/tmp/videos_naive'):
            shutil.rmtree('/tmp/videos_naive')

    c = VideoStream(src)
    pipelines = c[Null()]
    result = count(pipelines, ['one'], stats=True)

    logrecord('naive',({'file': src}), 'get', str(result), 's')


def runSimple(src, cleanUp = False):
    if cleanUp:
        if os.path.exists('/tmp/videos_simple'):
            shutil.rmtree('/tmp/videos_simple')

    manager = SimpleStorageManager('/tmp/videos_simple')
    now = timer()
    manager.put(src, os.path.basename(src), args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 50})
    put_time = timer() - now
    logrecord('simple', ({'file': src}), 'put', str({'elapsed': put_time}), 's')

    clips = manager.get(os.path.basename(src), lambda f: True)
    pipelines = []
    for c in clips:
        pipelines.append(c[Null()])
    result = counts(pipelines, ['one'], stats=True)
    logrecord('simple', ({'file': src}), 'get', str(result), 's')

def runFullSequential(src, cleanUp = False):
    if cleanUp:
        if os.path.exists('/tmp/videos_full'):
            shutil.rmtree('/tmp/videos_full')

    manager = FullStorageManager(None, CropSplitter(), '/tmp/videos_full')
    now = timer()
    manager.put(src, os.path.basename(src), parallel = False, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 50, 'num_processes': os.cpu_count()})
    put_time = timer() - now
    logrecord('full', ({'file': src}), 'put', str({'elapsed': put_time}), 's')

    clips = manager.get(os.path.basename(src), Condition())
    pipelines = []
    for c in clips:
        pipelines.append(c[Null()])
    result = counts(pipelines, ['one'], stats=True)
    logrecord('full', ({'file': src}), 'get', str(result), 's')

def runFullPutMany(src_list, cleanUp = False):
    if cleanUp:
        if os.path.exists('/tmp/videos_full'):
            shutil.rmtree('/tmp/videos_full')

    manager = FullStorageManager(None, CropSplitter(), '/tmp/videos_full')
    now = timer()
    targets = [os.path.basename(src) for src in src_list]
    logs = manager.put_many(src_list, targets, log = True, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 50, 'num_processes': os.cpu_count()})
    put_time = timer() - now
    logrecord('full', ({'file': src_list}), 'put', str({'elapsed': put_time}), 's')
    for i, log in enumerate(logs):
        logrecord('fullMany', i, 'put', str({'elapsed': log}), 's')

    # Don't call get() for now
    for src in src_list:
        clips = manager.get(os.path.basename(src), Condition())
        pipelines = []
        for c in clips:
            pipelines.append(c[Null()])
        result = counts(pipelines, ['one'], stats=True)
        logrecord('full', ({'file': src}), 'get', str(result), 's')

print("Number of CPUs: ", os.cpu_count())
total_start = timer()
for item in youtube_ids2:
    try:
        video_path="./deeplens/media/train/"+item+".mp4"
        runNaive(video_path)
    except:
        print("missing file for naive", item)
print("Total time for naive:", timer() - total_start)

total_start = timer()
for item in youtube_ids2:
    try:
        video_path="./deeplens/media/train/"+item+".mp4"
        runSimple(video_path, cleanUp=False)
    except:
        print("missing file for simple", item)
print("Total time for simple (cleanUp = False):", timer() - total_start)

total_start = timer()
for item in youtube_ids2:
    try:
        video_path="./deeplens/media/train/"+item+".mp4"
        runSimple(video_path, cleanUp=True)
    except:
        print("missing file for simple", item)
print("Total time for simple (cleanUp = True):", timer() - total_start)

total_start = timer()
for item in youtube_ids2:
    try:
        video_path="./deeplens/media/train/"+item+".mp4"
        runFullSequential(video_path, cleanUp=False)
    except:
        print("missing file for full", item)
print("Total time for full without parallelism within a video (cleanUp = False):", timer() - total_start)

total_start = timer()
for item in youtube_ids2:
    try:
        video_path="./deeplens/media/train/"+item+".mp4"
        runFullSequential(video_path, cleanUp=True)
    except:
        print("missing file for full", item)
print("Total time for full without parallelism within a video (cleanUp = True):", timer() - total_start)

total_start = timer()
runFullPutMany(["./deeplens/media/train/"+item+".mp4" for item in youtube_ids2], cleanUp=False)
print("Total time for full with parallelism across videos (cleanUp = False):", timer() - total_start)


total_start = timer()
runFullPutMany(["./deeplens/media/train/"+item+".mp4" for item in youtube_ids2], cleanUp=True)
print("Total time for full with parallelism across videos (cleanUp = True):", timer() - total_start)