
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.video.tracking.background import FixedCameraBGFGSegmenter
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *
from deeplens.utils.testing_utils import *


manager = FullStorageManager(FixedCameraBGFGSegmenter().segment, CropSplitter(), 'videos')
manager.put('./videos/cut3.mp4', 'test2')
#res = manager.get('test', Condition(label='foreground', crop=Box(0, 0, 1000, 1000)))
#print([video for video in res[0]][0]['data'].shape)
#print(len(res))
#play(res[0])

