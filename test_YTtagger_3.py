import os
import shutil

import pandas as pd
from deeplens.dataflow.agg import count, counts
from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_manager import FullStorageManager
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.constants import *
from deeplens.media.youtube_tagger import YoutubeTagger
from deeplens.simple_manager.manager import SimpleStorageManager
from deeplens.struct import VideoStream
from deeplens.tracking.contour import KeyPoints
from experiments.environ import logrecord
from timeit import default_timer as timer


def runFull(src, cleanUp = False):
    if cleanUp:
        if os.path.exists('./videos_full'):
            shutil.rmtree('./videos_full')

    manager = FullStorageManager(None, CropSplitter(), 'videos_full')
    now = timer()
    manager.put(src, os.path.basename(src), parallel = False, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 50, 'num_processes': os.cpu_count()})
    put_time = timer() - now
    logrecord('full', ({'file': src}), 'put', str({'elapsed': put_time}), 's')

def runFullPutMany(src_list, cleanUp = False):
    if cleanUp:
        if os.path.exists('./videos_full'):
            shutil.rmtree('./videos_full')

    manager = FullStorageManager(None, CropSplitter(), 'videos_full')
    now = timer()
    targets = [os.path.basename(src) for src in src_list]
    logs = manager.put_many(src_list, targets, log = True, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 50, 'num_processes': os.cpu_count()})
    put_time = timer() - now
    logrecord('full', ({'file': src_list}), 'put', str({'elapsed': put_time}), 's')
    for i, log in enumerate(logs):
        logrecord('fullMany', i, 'put', str({'elapsed': log}), 's')


df = pd.read_csv('./deeplens/media/train/processed_yt_bb_detection_train.csv', sep=',',
                 dtype={'youtube_id': str})
youtube_ids=df['youtube_id']
youtube_ids2=list(dict.fromkeys(youtube_ids))


print("Number of CPUs: ", os.cpu_count())
total_start = timer()
for item in youtube_ids2:
    try:
        video_path="./deeplens/media/train/"+item+".mp4"
        runFull(video_path, cleanUp=False)
    except:
        print("missing file for full", item)
print("Total time for full without parallelism within a video (cleanUp = False):", timer() - total_start)

total_start = timer()
for item in youtube_ids2:
    try:
        video_path="./deeplens/media/train/"+item+".mp4"
        runFull(video_path, cleanUp=True)
    except:
        print("missing file for full", item)
print("Total time for full without parallelism within a video (cleanUp = True):", timer() - total_start)

total_start = timer()
runFullPutMany(["./deeplens/media/train/"+item+".mp4" for item in youtube_ids2], cleanUp=False)
print("Total time for full with parallelism across videos (cleanUp = False):", timer() - total_start)


total_start = timer()
runFullPutMany(["./deeplens/media/train/"+item+".mp4" for item in youtube_ids2], cleanUp=True)
print("Total time for full with parallelism across videos (cleanUp = True):", timer() - total_start)
