from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.tracking.contour import *
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *
from deeplens.full_manager.video_processing_keypoint import *


manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=30), CropSplitter(), 'videos')
manager = FullStorageManager(SizeMovementTagger(), CropSplitter(), 'videos')
manager.put('./cut3.mp4', 'test2')
#res = manager.get('test', Condition(label='small'))
#print([video for video in res[0]][0]['data'].shape)
#print(len(res))
#play(res[0])

