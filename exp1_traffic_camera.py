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

	c = VideoStream("tcam.mp4", limit=tot)
	sel = sel/2
	region = Box(200,550,350,750)
	pipelines = c[Cut(500-int(tot*sel),500+int(tot*sel))][KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
	print('Naive', count(pipelines, ['one'], stats=True))


#Simple storage manager with temporal filters
def runSimple(tot=1000, sel=0.1):
	cleanUp()

	manager = SimpleStorageManager('videos')
	manager.put('tcam.mp4', 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 20})

	region = Box(200,550,350,750)

	sel = sel/2

	clips = manager.get('test',lambda f: overlap(f['start'],f['end'],500-int(tot*sel),500+int(tot*sel)))
	pipelines = []
	for c in clips:
		pipelines.append(c[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)])

	print('Simple', counts(pipelines, ['one'], stats=True))


#Full storage manager with bg-fg optimization
def runFull(tot=1000, sel=0.1):
	cleanUp()

	manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=100), CropSplitter(), 'videos')
	manager.put('tcam.mp4', 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': 1000, 'batch_size': 100})

	sel = sel/2

	clips = manager.get('test', 'foreground',time_filter(500-int(tot*sel),500+int(tot*sel)))
	pipelines = []

	for c in clips:
		pipelines.append(c[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)])

	print('Full', counts(pipelines, ['one'], stats=True))


#All optimizations
def runFullOpt(tot=1000, sel=0.1):
	cleanUp()

	manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=100), CropSplitter(), 'videos')
	manager.put('tcam.mp4', 'test', args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': 1000, 'batch_size': 100})

	sel = sel/2
	
	clips = manager.get('test', 'foreground',time_filter(500-int(tot*sel),500+int(tot*sel)))

	pipelines = []
	for c in clips:
		pipeline = c[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25,-0.25,1,-0.25,-0.25],1.5, delay=10)]
		pipeline = d.optimize(pipeline)
		pipelines.append(pipeline)

	print('FullOpt', counts(pipelines, ['one'], stats=True))


runNaive(tot=1000, sel=0.1)
runSimple(tot=1000, sel=0.1)
runFull(tot=1000, sel=0.1)
