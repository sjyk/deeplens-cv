from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *

from deeplens.dataflow.agg import *
from deeplens.tracking.contour import *
from deeplens.tracking.event import *

#from deeplens.extern.ffmpeg import *
#from deeplens


#manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=300), CropSplitter(), 'videos')
#manager.put('tcam.mp4', 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': 1000, 'batch_size': 100, 'num_processes': 4, 'background_scale': 1})
#clips = manager.get('test', Condition(label='foreground'))


"""
for i in range(25,60,3):
	new_file = set_bitrate('tcam.mp4',str(i)+'tcam.avi', i)
	c = VideoStream(new_file, limit=1000)
	region = Box(200,550,350,750)
	pipelines = c[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
	result = count(pipelines, ['one'], stats=True)
	print('Bitrate',i,result)
"""

#for i in range(25,60,3):
#	print(str(i)+'tcam.avi', get_bitrate(str(i)+'tcam.avi'))

#region = Box(200,550,350,750)
from deeplens.optimizer.deeplens import DeepLensOptimizer
d = DeepLensOptimizer()
v = [VideoStream('58tcam.avi', limit=1000), VideoStream('25tcam.avi', limit=1000)] 
c = IteratorVideoStream(itertools.chain(*v), v)
d.optimize(c)

#pipelines = c[KeyPoints(blur=1)][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
#print(count(pipelines, ['one'], stats=True))