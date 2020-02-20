from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.struct import *
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.utils import *
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *
from deeplens.utils.testing_utils import *
from deeplens.dataflow.agg import *
from deeplens.tracking.contour import *
from deeplens.tracking.event import *
from deeplens.core import *

from deeplens.simple_manager.manager import *

from deeplens.optimizer.deeplens import DeepLensOptimizer

import cv2
import numpy as np
import os
import shutil


def time_filter(start, end):

	def do_filter(conn, video_name):
		c = conn.cursor()
		c.execute("SELECT clip_id FROM clip WHERE start_time >= %s AND end_time <= %s AND video_name = '%s'" % (str(start), str(end), video_name))
		return [cl[0] for cl in c.fetchall()]

	return do_filter

def overlap(s1,e1, s2, e2):
	r1 = set(range(s1,e1))
	r2 = set(range(s2,e2))
	return (len(r1.intersection(r2)) > 0)


def cleanUp():
	if os.path.exists('./videos'):
		shutil.rmtree('./videos')


#loads directly from the mp4 file
def runNaive(tot=1000, sel=0.1):
	cleanUp()

	c = VideoStream('/Users/sanjaykrishnan/Downloads/panoramic-000-000.mp4', limit=tot)
	sel = sel/2
	region = Box(500, 350, 750, 450)
	pipelines = c[Cut(tot//2-int(tot*sel),tot//2+int(tot*sel))][KeyPoints(blur=3)][ActivityMetric('one', region)][Filter('one', [-0.5,1,-0.5], 0.25, delay=40)]

	print('Naive.'+str(tot)+"."+str(sel), count(pipelines, ['one'], stats=True)[1])


#Simple storage manager with temporal filters
def runSimple(tot=1000, sel=0.1):
	cleanUp()

	manager = SimpleStorageManager('videos')
	manager.put('/Users/sanjaykrishnan/Downloads/panoramic-000-000.mp4', 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 20})

	region = Box(500, 350, 750, 450)

	sel = sel/2

	clips = manager.get('test',lambda f: overlap(f['start'],f['end'],tot//2-int(tot*sel),tot//2+int(tot*sel)))
	pipelines = []
	for c in clips:
		pipelines.append(c[KeyPoints(blur=3)][ActivityMetric('one', region)][Filter('one', [-0.5,1,-0.5], 0.25, delay=40)])

	print('Simple.'+str(tot)+"."+str(sel), counts(pipelines, ['one'], stats=True)[1])


#Full storage manager with bg-fg optimization
def runFull(tot=1000, sel=0.1):
	cleanUp()

	manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter(movement_threshold=21,blur=7,movement_prob=0.9).segment, batch_size=100), CropSplitter(), 'videos')
	manager.put('/Users/sanjaykrishnan/Downloads/panoramic-000-000.mp4', 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': 1000, 'batch_size': 100})

	region = Box(500, 350, 750, 450)
	sel = sel/2

	clips = manager.get('test', Condition(label='foreground', custom_filter=time_filter(tot//2-int(tot*sel),tot//2+int(tot*sel))))
	pipelines = []

	for c in clips:
		pipelines.append(c[KeyPoints(blur=3)][ActivityMetric('one', region)][Filter('one', [-0.5,1,-0.5], 0.25, delay=40)])

	print('Full.'+str(tot)+"."+str(sel), counts(pipelines, ['one'], stats=True)[1])


#All optimizations
def runFullOpt(tot=1000, sel=0.1):
	cleanUp()

	manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter(movement_threshold=21,blur=7,movement_prob=0.9).segment, batch_size=100), CropSplitter(), 'videos')
	manager.put('/Users/sanjaykrishnan/Downloads/panoramic-000-000.mp4', 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': 1000, 'batch_size': 100})

	region = Box(500, 350, 750, 450)
	sel = sel/2
	
	clips = manager.get('test', Condition(label='foreground',custom_filter=time_filter(tot//2-int(tot*sel),tot//2+int(tot*sel))))

	pipelines = []
	d = DeepLensOptimizer(crop_pd=False)
	for c in clips:
		pipeline = c[KeyPoints(blur=3)][ActivityMetric('one', region)][Filter('one', [-0.5,1,-0.5], 0.25, delay=40)]
		pipeline = d.optimize(pipeline)
		pipelines.append(pipeline)

	print('FullOpt.'+str(tot)+"."+str(sel), counts(pipelines, ['one'], stats=True)[1])


N = 1000
for si in range(2,10):
		s = si/10
		runNaive(tot=N, sel=s)
		runSimple(tot=N, sel=s)
		runFull(tot=N, sel=s)
		runFullOpt(tot=N, sel=s)