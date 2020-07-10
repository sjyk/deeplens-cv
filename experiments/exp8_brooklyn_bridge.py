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


# loads directly from the mp4 file
def runNaive(src, tot=1000, sel=0.1):
    cleanUp()

    c = VideoStream(src, limit=tot)
    sel = sel / 2
    left = Box(1500, 1600, 1750, 1800)
    middle = Box(1750, 1600, 2000, 1800)
    right = Box(2000, 1600, 2250, 1800)
    pipelines = c[Cut(tot // 2 - int(tot * sel), tot // 2 + int(tot * sel))][GoodKeyPoints()][ActivityMetric('left', left)][
        ActivityMetric('middle', middle)][ActivityMetric('right', right)][
        Filter('left', [1,1,1], 3, delay=10)][
        Filter('middle', [1,1,1], 3, delay=10)][
        Filter('right', [1,1,1], 3, delay=10)]

    result = count(pipelines, ['left', 'middle', 'right'], stats=True)

    logrecord('naive', ({'size': tot, 'sel': sel, 'file': src}), 'get', str(result), 's')


# Simple storage manager with temporal filters
def runSimple(src, tot=1000, sel=0.1):
    cleanUp()

    manager = SimpleStorageManager('videos')
    now = timer()
    manager.put(src, 'test',
                args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 20,
                      'num_processes': 4})
    put_time = timer() - now
    print("Put time for simple:", put_time)

    left = Box(1500, 1600, 1750, 1800)
    middle = Box(1750, 1600, 2000, 1800)
    right = Box(2000, 1600, 2250, 1800)

    sel = sel / 2

    clips = manager.get('test',
                        lambda f: overlap(f['start'], f['end'], tot // 2 - int(tot * sel), tot // 2 + int(tot * sel)))
    pipelines = []
    for c in clips:
        pipelines.append(c[GoodKeyPoints()][ActivityMetric('left', left)][
                             ActivityMetric('middle', middle)][ActivityMetric('right', right)][
                             Filter('left', [1,1,1], 3, delay=10)][
                             Filter('middle', [1,1,1], 3, delay=10)][
                             Filter('right', [1,1,1], 3, delay=10)])

    result = counts(pipelines, ['left', 'middle', 'right'], stats=True)

    logrecord('simple', ({'size': tot, 'sel': sel, 'file': src}), 'get', str(result), 's')


# Full storage manager with bg-fg optimization
def runFull(src, tot=1000, sel=0.1):
    cleanUp()

    manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=20), CropSplitter(),
                                 'videos')
    now = timer()
    manager.put(src, 'test',
                args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 20,
                      'num_processes': 4, 'background_scale': 1})
    put_time = timer() - now
    print("Put time for full:", put_time)

    left = Box(1500, 1600, 1750, 1800)
    middle = Box(1750, 1600, 2000, 1800)
    right = Box(2000, 1600, 2250, 1800)
    sel = sel / 2

    clips = manager.get('test', Condition(label='foreground', custom_filter=time_filter(tot // 2 - int(tot * sel),
                                                                                        tot // 2 + int(tot * sel))))
    pipelines = []

    for c in clips:
        pipelines.append(c[GoodKeyPoints()][ActivityMetric('left', left)][
                             ActivityMetric('middle', middle)][ActivityMetric('right', right)][
                             Filter('left', [1,1,1], 3, delay=10)][
                             Filter('middle', [1,1,1], 3, delay=10)][
                             Filter('right', [1,1,1], 3, delay=10)])

    result = counts(pipelines, ['left', 'middle', 'right'], stats=True)

    logrecord('full', ({'size': tot, 'sel': sel, 'file': src}), 'get', str(result), 's')


# All optimizations
def runFullOpt(src, tot=1000, sel=0.1):
    cleanUp()

    manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=20), CropSplitter(),
                                 'videos')
    now = timer()
    manager.put(src, 'test',
                args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': 20,
                      'num_processes': 4, 'background_scale': 1})
    put_time = timer() - now
    print("Put time for full opt:", put_time)

    left = Box(1500, 1600, 1750, 1800)
    middle = Box(1750, 1600, 2000, 1800)
    right = Box(2000, 1600, 2250, 1800)
    sel = sel / 2

    clips = manager.get('test', Condition(label='foreground', custom_filter=time_filter(tot // 2 - int(tot * sel),
                                                                                        tot // 2 + int(tot * sel))))

    pipelines = []
    d = DeepLensOptimizer()
    for c in clips:
        pipeline = c[GoodKeyPoints()][ActivityMetric('left', left)][
            ActivityMetric('middle', middle)][ActivityMetric('right', right)][
            Filter('left', [1,1,1], 3, delay=10)][
            Filter('middle', [1,1,1], 3, delay=10)][
            Filter('right', [1,1,1], 3, delay=10)]
        pipeline = d.optimize(pipeline)
        pipelines.append(pipeline)

    result = counts(pipelines, ['left', 'middle', 'right'], stats=True)

    logrecord('fullopt', ({'size': tot, 'sel': sel, 'file': src}), 'get', str(result), 's')


logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')
do_experiments(sys.argv[1], [runNaive, runSimple, runFull, runFullOpt], 600, range(2, 3))