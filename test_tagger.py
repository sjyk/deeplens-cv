from deeplens.full_manager.full_video_processing import *
from deeplens.struct import *
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.utils import *
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *
from deeplens.utils.testing_utils import *
import os
import shutil
import time

# logging.basicConfig(level=logging.DEBUG)
if os.path.exists('/tmp/videos'):
    shutil.rmtree('/tmp/videos')
manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=20), CropSplitter(), '/tmp/videos')
start = time.time()
output = manager.put('./tcam.mp4' , 'test', parallel = False, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': 1000, 'batch_size': 20, 'num_processes': 8, 'background_scale': 1})
end = time.time()
print("Without background resizing:", end - start)

if os.path.exists('/tmp/videos'):
    shutil.rmtree('/tmp/videos')
manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=20), CropSplitter(), '/tmp/videos')
start = time.time()
output = manager.put('./tcam.mp4' , 'test', parallel = False, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': 1000, 'batch_size': 20, 'num_processes': 8, 'background_scale': 0.2})
end = time.time()
print("With background resizing:", end - start)