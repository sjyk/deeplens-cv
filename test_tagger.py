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

if os.path.exists('./videos'):
    shutil.rmtree('./videos')
manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=5), CropSplitter(), 'videos')
start = time.time()
manager.put('./BigBuckBunny.mp4', 'test', parallel = False, args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 20, 'num_processes': 4})
end = time.time()
print(end - start)