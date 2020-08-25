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
    left = Box(1600, 1600, 1700, 1800)
    middle = Box(1825, 1600, 1975, 1800)
    right = Box(2050, 1600, 2175, 1800)
    # left = Box(1600/3, 1600/3, 1700/3, 1800/3)
    # middle = Box(1825/3, 1600/3, 1975/3, 1800/3)
    # right = Box(2050/3, 1600/3, 2175/3, 1800/3)

    # left = Box(1600 / 2, 1600 / 2, 1700 / 2, 1800 / 2)
    # middle = Box(1825 / 2, 1600 / 2, 1975 / 2, 1800 / 2)
    # right = Box(2050 / 2, 1600 / 2, 2175 / 2, 1800 / 2)
    pipelines = c[GoodKeyPoints()][ActivityMetric('left', left)][
        ActivityMetric('middle', middle)][ActivityMetric('right', right)][
        Filter('left', [1], 1, delay=25)][
        Filter('middle', [1], 1, delay=25)][
        Filter('right', [1], 1, delay=25)]

    result = count(pipelines, ['left', 'middle', 'right'], stats=True)

    logrecord('naive', ({'size': tot, 'sel': sel, 'file': src}), 'get', str(result), 's')


# Full storage manager with bg-fg optimization
def runFull(src, tot=1000, batch_size=20):
    cleanUp()

    folder = '/tmp/videos'

    def tagger(vstream, batch_size):
        count = 0
        for frame in vstream:
            count += 1
            if count >= batch_size:
                break
        if count == 0:
            raise StopIteration("Iterator is closed")
        return {'label': 'foreground', 'bb': Box(1600, 1600, 2175, 1800)}

    def put():
        manager = FullStorageManager(CustomTagger(tagger, batch_size=batch_size), CropSplitter(do_join=False),
                                     folder, dsn='dbname=header user=postgres password=deeplens host=10.0.0.5')
        now = timer()
        manager.put(src, 'test',
                    args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': tot, 'batch_size': batch_size,
                          'num_processes': 4, 'background_scale': 1})
        put_time = timer() - now
        print("Put time for full:", put_time)
        print("Batch size:", batch_size, "Folder size:", get_size(folder))

    def get():
        left = Box(1600, 1600, 1700, 1800)
        middle = Box(1825, 1600, 1975, 1800)
        right = Box(2050, 1600, 2175, 1800)

        # left = Box(1600 / 3, 1600 / 3, 1700 / 3, 1800 / 3)
        # middle = Box(1825 / 3, 1600 / 3, 1975 / 3, 1800 / 3)
        # right = Box(2050 / 3, 1600 / 3, 2175 / 3, 1800 / 3)

        # left = Box(1600 / 2, 1600 / 2, 1700 / 2, 1800 / 2)
        # middle = Box(1825 / 2, 1600 / 2, 1975 / 2, 1800 / 2)
        # right = Box(2050 / 2, 1600 / 2, 2175 / 2, 1800 / 2)


        clips = manager.get('test', Condition(label='foreground', custom_filter=None))
        pipelines = []

        now = timer()
        frame_count = 0
        for c in clips:
            for frame in c:
                frame_count += 1
            # pipelines.append(c[GoodKeyPoints()][ActivityMetric('left', left)][
            #                      ActivityMetric('middle', middle)][ActivityMetric('right', right)][
            #                      Filter('left', [1], 1, delay=25)][
            #                      Filter('middle', [1], 1, delay=25)][
            #                      Filter('right', [1], 1, delay=25)])

        # result = counts(pipelines, ['left', 'middle', 'right'], stats=True)
        result = timer() - now
        print(frame_count)

        logrecord('full', ({'size': tot, 'batch_size': batch_size, 'file': src, 'folder_size': get_size(folder)}), 'get', str(result), 's')

    put()


logging.basicConfig(level=logging.INFO, format='%(asctime)-15s %(message)s')
#do_experiments(sys.argv[1], [runNaive, runSimple, runFull, runFullOpt], 600, range(9, 10))
do_experiments_batch_size(sys.argv[1], [runFull], -1, [72])