import os
import shutil
import time

from deeplens.dataflow.agg import counts
from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_manager import FullStorageManager
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.media.youtube_tagger import YoutubeTagger
from deeplens.constants import *
from deeplens.tracking.contour import KeyPoints
from experiments.environ import logrecord

"""
I literally just want to make sure this iterator does what I think it does
I'm not going to worry about whether it's compatible with the storage manager
for now...
"""

def test_iteration(src):
    youtubeTagger = YoutubeTagger(src, './train/processed_yt_bb_detection_train.csv')
    for frame in youtubeTagger:
        bb = frame['objects'][0]['bb']
        bbstr = str(bb.x0) + ',' + str(bb.x1) + ',' + str(bb.y0) + ',' + str(bb.y1)
        print(bbstr)
        print(frame['objects'][0]['label'])

def test_put(src, cleanUp = False):
    if cleanUp:
        if os.path.exists('./videos'):
            shutil.rmtree('./videos')
    manager = FullStorageManager(None, CropSplitter(), 'videos')
    start = time.time()
    manager.put(src, os.path.basename(src), parallel = True, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 50, 'num_processes': 6})
    print("Put time:", time.time() - start)
    clips = manager.get(os.path.basename(src), Condition(label='person'))
    pipelines = []
    for c in clips:
        pipelines.append(c[KeyPoints()])
    result = counts(pipelines, ['one'], stats=True)
    logrecord('full', ({'file': src}), 'get', str(result), 's')

test_put('./train/AACM71csS-Q.mp4', cleanUp=False)
