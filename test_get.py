from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.tracking.contour import *
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *
from deeplens.full_manager.video_processing_keypoint import *
from deeplens.object_detection.detect import *


#manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=30), CropSplitter(), 'videos')
#manager = FullStorageManager(SizeMovementTagger(), CropSplitter(), 'videos')
manager = FullStorageManager(TensorFlowObjectDetect(model_file='ssd_mobilenet_v1_coco_2017_11_17', label_file='mscoco_label_map.pbtxt',
                               num_classes=90, confidence=0.25), CropSplitter(), 'videos')
manager.put('./cut3.mp4', 'test2')
#res = manager.get('test', Condition(label='small'))
#print([video for video in res[0]][0]['data'].shape)
#print(len(res))
#play(res[0])

