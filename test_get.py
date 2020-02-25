from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *


manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=20), CropSplitter(), 'videos')
res = manager.get('test', Condition(label='foreground', crop=Box(0, 0, 1000, 1000)))
print([video for video in res[0]][0]['data'].shape)
print(len(res))
play(res[0])

