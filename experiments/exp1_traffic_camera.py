import environ
import sys
from environ import *

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

import cv2
import numpy as np


#loads directly from the mp4 file
def runNaive(src, tot=1000, sel=0.1):
	cleanUp()

	c = VideoStream(src, limit=tot)
	sel = sel/2
	region = Box(200,550,350,750)
	pipelines = c[Cut(tot//2-int(tot*sel),tot//2+int(tot*sel))][KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
	result = count(pipelines, ['one'], stats=True)[1]['elapsed']

	logrecord('naive',({'size': tot, 'sel': sel, 'file': src}), 'get', str(result), 's')


#Simple storage manager with temporal filters
def runSimple(src, tot=1000, sel=0.1):
	cleanUp()

	manager = SimpleStorageManager('videos')
	manager.put(src, 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 20})

	region = Box(200,550,350,750)

	sel = sel/2

	clips = manager.get('test',lambda f: overlap(f['start'],f['end'],tot//2-int(tot*sel),tot//2+int(tot*sel)))
	pipelines = []
	for c in clips:
		pipelines.append(c[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)])

	result = counts(pipelines, ['one'], stats=True)[1]['elapsed']

	logrecord('simple',({'size': tot, 'sel': sel, 'file': src}), 'get', str(result), 's')


#Full storage manager with bg-fg optimization
def runFull(src, tot=1000, sel=0.1):
	cleanUp()

	manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=100), CropSplitter(), 'videos')
	manager.put(src, 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 100})

	region = Box(200, 550, 350, 750)
	sel = sel/2

	clips = manager.get('test', Condition(label='foreground', custom_filter=time_filter(tot//2-int(tot*sel),tot//2+int(tot*sel))))
	pipelines = []

	for c in clips:
		pipelines.append(c[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)])

	result = counts(pipelines, ['one'], stats=True)[1]['elapsed']

	logrecord('full',({'size': tot, 'sel': sel, 'file': src}), 'get', str(result), 's')



#All optimizations
def runFullOpt(src, tot=1000, sel=0.1):
	cleanUp()

	manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=100), CropSplitter(), 'videos')
	manager.put(src, 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 100})

	region = Box(200, 550, 350, 750)
	sel = sel/2
	
	clips = manager.get('test', Condition(label='foreground',custom_filter=time_filter(tot//2-int(tot*sel),tot//2+int(tot*sel))))

	pipelines = []
	d = DeepLensOptimizer()
	for c in clips:
		pipeline = c[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
		pipeline = d.optimize(pipeline)
		pipelines.append(pipeline)

	result = counts(pipelines, ['one'], stats=True)[1]['elapsed']

	logrecord('fullopt',({'size': tot, 'sel': sel, 'file': src}), 'get', str(result), 's')


do_experiments(sys.argv[1], [runNaive, runSimple, runFull, runFullOpt], 1000, range(2,10))