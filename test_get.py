from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *

from deeplens.dataflow.agg import *
from deeplens.tracking.contour import *
from deeplens.tracking.event import *
#from deeplens


#manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=300), CropSplitter(), 'videos')
#manager.put('tcam.mp4', 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': 1000, 'batch_size': 100, 'num_processes': 4, 'background_scale': 1})
#clips = manager.get('test', Condition(label='foreground'))


c = VideoStream('tcam.mp4')
region = Box(200,550,350,750)
pipelines = c[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
result = count(pipelines, ['one'], stats=True)
print(result)