import os
import sys
import inspect

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import *
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.full_manager.full_manager import *
import shutil
import time
import urllib.request

def test_tagger():
    # logging.basicConfig(level=logging.DEBUG)
    urllib.request.urlretrieve("https://www.dropbox.com/s/6adl0d91l5lokbu/cut4.mp4?dl=1", "/tmp/test.mp4")

    if os.path.exists('/tmp/videos'):
        shutil.rmtree('/tmp/videos')
    manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=20), CropSplitter(), '/tmp/videos', dsn='dbname=postgres user=postgres password=postgres host=127.0.0.1')
    start = time.time()
    output = manager.put('/tmp/test.mp4' , 'test', parallel = False, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': 1000, 'batch_size': 20, 'num_processes': 8, 'background_scale': 1})
    end = time.time()
    print("Without background resizing:", end - start)
    res = manager.get('test', Condition(label='foreground'))
    shape1 = next(next(res))['data'].shape
    assert shape1[2] == 3

    manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=20), CropSplitter(), '/tmp/videos', dsn='dbname=postgres user=postgres password=postgres host=127.0.0.1')
    start = time.time()
    output = manager.put('/tmp/test.mp4' , 'test', parallel = False, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': 1000, 'batch_size': 20, 'num_processes': 8, 'background_scale': 0.2})
    end = time.time()
    print("With background resizing:", end - start)
    res = manager.get('test', Condition(label='foreground'))
    shape2 = next(next(res))['data'].shape
    assert shape2[2] == 3

if __name__ == '__main__':
    test_tagger()