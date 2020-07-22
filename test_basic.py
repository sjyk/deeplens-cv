
from deeplens.full_manager.full_video_processing import CropUnionSplitter
from deeplens.video.tracking.background import FixedCameraBGFGSegmenter
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *


manager = FullStorageManager(FixedCameraBGFGSegmenter().segment, CropUnionSplitter(), 'videos2')
#manager.put('./videos/cut4.mp4', 'test2')
res = manager.get("SELECT * FROM clip WHERE video_name = '%s'" %('test2'))
print(res)
#print([video for video in res[0]][0]['data'].shape)
#print(len(res))
#play(res[0])

