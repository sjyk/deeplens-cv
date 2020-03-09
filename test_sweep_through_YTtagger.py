import os
import shutil
import time

import pandas as pd
from deeplens.dataflow.agg import counts
from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_manager import FullStorageManager
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.media.youtube_tagger import YoutubeTagger
from deeplens.constants import *
from experiments.environ import logrecord


def test_put(src, cleanUp = False):
    if cleanUp:
        if os.path.exists('./videos'):
            shutil.rmtree('./videos')
    youtubeTagger = YoutubeTagger(src, './deeplens/media/train/processed_yt_bb_detection_train.csv')
    manager = FullStorageManager(youtubeTagger, CropSplitter(), 'videos')
    start = time.time()
    manager.put(src, 'test', parallel = False, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 20, 'num_processes': 4})
    print("Put time:", time.time() - start)
    clips = manager.get('test', Condition(label='person'))
    pipelines = []
    for c in clips:
        pipelines.append(c)
    result = counts(pipelines, ['one'], stats=True)
    logrecord('full', ({'file': src}), 'get', str(result), 's')

#test_put('./deeplens/media/train/AAI0cDTWFvE.mp4')


df = pd.read_csv('./deeplens/media/train/processed_yt_bb_detection_train.csv', sep=',',
                 dtype={'youtube_id': str})
youtube_ids=df['youtube_id']
youtube_ids2=list(dict.fromkeys(youtube_ids))

whole_start = time.time()
for item in youtube_ids2:
    try:
        video_path="./deeplens/media/train/"+item+".mp4"
        test_put(video_path)
    except:
        print("missing file",item)
print("whole_put_get_time:",time.time()-whole_start)
