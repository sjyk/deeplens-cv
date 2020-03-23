import sys
from experiments.environ import *

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


# loads directly from the mp4 file
def runNaive(src, tot=-1, sel=0.1):
    cleanUp()

    c = VideoStream(src, limit=tot)
    sel = sel / 2
    region = Box(515, 200, 700, 600)
    pipelines = \
    c[KeyPoints()][ActivityMetric('one', region)][
        Filter('one', [-0.25, -0.25, 1, -0.25, -0.25], 1.5, delay=10)]
    result = count(pipelines, ['one'], stats=True)

    logrecord('naive', ({'file': src}), 'get', str(result), 's')

def runNaiveOpt(src, tot=-1, sel=0.1):
    cleanUp()

    c = VideoStream(src, limit=tot)
    sel = sel / 2
    region = Box(515, 200, 700, 600)
    pipelines = \
    c[KeyPoints()][ActivityMetric('one', region)][
        Filter('one', [-0.25, -0.25, 1, -0.25, -0.25], 1.5, delay=10)]
    d = DeepLensOptimizer()
    pipelines = d.optimize(pipelines)
    result = count(pipelines, ['one'], stats=True)

    logrecord('naive', ({'file': src}), 'get', str(result), 's')


# Simple storage manager with temporal filters
def runSimple(src, tot=-1, sel=0.1):
    cleanUp()

    manager = SimpleStorageManager('videos')
    now = timer()
    manager.put(src, 'test',
                args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 100,
                      'num_processes': 4})
    put_time = timer() - now
    print("Put time for simple:", put_time)

    region = Box(515, 200, 700, 600)

    sel = sel / 2

    clips = manager.get('test', lambda f: True)
    pipelines = []
    for c in clips:
        pipelines.append(c[KeyPoints()][ActivityMetric('one', region)][
                             Filter('one', [-0.25, -0.25, 1, -0.25, -0.25], 1.5, delay=10)])

    result = counts(pipelines, ['one'], stats=True)

    logrecord('simple', ({'file': src}), 'get', str(result), 's')


# Simple storage manager with temporal filters
def runSimpleOpt(src, tot=-1, sel=0.1):
    cleanUp()

    manager = SimpleStorageManager('videos')
    now = timer()
    manager.put(src, 'test',
                args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 100,
                      'num_processes': 4})
    put_time = timer() - now
    print("Put time for simple:", put_time)

    region = Box(515, 200, 700, 600)

    sel = sel / 2

    clips = manager.get('test', lambda f: True)
    pipelines = []
    for c in clips:
        pipeline = c[KeyPoints()][ActivityMetric('one', region)][
                             Filter('one', [-0.25, -0.25, 1, -0.25, -0.25], 1.5, delay=10)]
        d = DeepLensOptimizer()
        pipelines.append(d.optimize(pipeline))

    result = counts(pipelines, ['one'], stats=True)

    logrecord('simple', ({'file': src}), 'get', str(result), 's')

# Full storage manager with bg-fg optimization
def runFull(src, tot=-1, sel=0.1):
    cleanUp()

    manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=100), CropSplitter(),
                                 'videos')
    now = timer()
    manager.put(src, 'test',
                args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 100,
                      'num_processes': 12})
    put_time = timer() - now
    print("Put time for simple:", put_time)

    region = Box(515, 200, 700, 600)
    sel = sel / 2

    clips = manager.get('test', Condition(label='foreground'))
    pipelines = []

    for c in clips:
        pipelines.append(c[KeyPoints()][ActivityMetric('one', region)][
                             Filter('one', [-0.25, -0.25, 1, -0.25, -0.25], 1.5, delay=10)])

    result = counts(pipelines, ['one'], stats=True)

    logrecord('full', ({'file': src}), 'get', str(result), 's')


# All optimizations
def runFullOpt(src, tot=-1, sel=0.1):
    cleanUp()

    manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=100), CropSplitter(),
                                 'vi deos')
    now = timer()
    manager.put(src, 'test',
                args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 100,
                      'num_processes': 12})
    put_time = timer() - now
    print("Put time for simple:", put_time)

    region = Box(515, 200, 700, 600)
    sel = sel / 2

    clips = manager.get('test', Condition(label='foreground'))

    pipelines = []
    d = DeepLensOptimizer()
    for c in clips:
        pipeline = c[KeyPoints()][ActivityMetric('one', region)][
            Filter('one', [-0.25, -0.25, 1, -0.25, -0.25], 1.5, delay=10)]
        pipeline = d.optimize(pipeline)
        pipelines.append(pipeline)

    result = counts(pipelines, ['one'], stats=True)

    logrecord('fullopt', ({'file': src}), 'get', str(result), 's')


logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')
do_experiments(sys.argv[1], [runSimpleOpt], -1, [1])