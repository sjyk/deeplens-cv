import sys


from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.optimizer.deeplens import DeepLensOptimizer

from deeplens.struct import *
from deeplens.utils import *
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *
from deeplens.utils.testing_utils import *
from deeplens.dataflow.agg import *
from deeplens.tracking.contour import *
from deeplens.tracking.event import *
from deeplens.core import *
from deeplens.simple_manager.manager import *

from deeplens.utils.ui import play

import cv2
import numpy as np

"""
c = VideoStream('/Volumes/RAMDisk/tcam.mp4', limit=150)
region = Box(200,550,350,750)
pipelines = c[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
result = count(pipelines, ['one'], stats=True)
print(result)
"""


#vstream = VideoStream('/Users/sanjayk/Dropbox/DeepLensTestVideos/tcam.mp4', limit=100)

#for v in vstream:
#    pass

"""
from deeplens.extern.cache import persist, load, materialize

c = materialize(c)
region = Box(200,550,350,750)
pipelines = c[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
result = count(pipelines, ['one'], stats=True)
print(result)
"""

#from deeplens.extern.cache import persist
#vstream = VideoStream('/Users/sanjayk/Dropbox/DeepLensTestVideos/tcam.mp4', limit=1000)
#size = persist(vstream, 'cache.npz')

#from deeplens.struct import RawVideoStream

#vstream = RawVideoStream('cache.npz', shape=(1000,1080,1920,3))

#for v in vstream:
#    pass

manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=100), CropSplitter(), 'videos')
manager.put('tcam.mp4', 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': 300, 'batch_size': 20, 'num_processes': 4})
manager.cache('test', Condition(label='foreground'))
clips = manager.get('test', Condition(label='foreground'))
#play(clips[0])

#


region = Box(200, 550, 350, 750)
	
#clips = manager.get('test', Condition(label='foreground'))

pipelines = []
#d = DeepLensOptimizer()
for c in clips:
	pipeline = c[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
	#pipeline = d.optimize(pipeline)
	pipelines.append(pipeline)

result = counts(pipelines, ['one'], stats=True)
print(result)

manager.uncache('test', Condition(label='foreground'))

