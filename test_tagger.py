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

print('test')
if os.path.exists('./videos'):
    shutil.rmtree('./videos')
manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=5), CropSplitter(), 'videos')
start = time.time()
output = manager.put_many(['./cut2.mp4','./cut3.mp4'] , ['test', 'test1'], log = True, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 20, 'num_processes': 8})
print(output)
print('test2')
end = time.time()
print(end - start)