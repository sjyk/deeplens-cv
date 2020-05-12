import sys
import environ

from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.optimizer.deeplens import DeepLensOptimizer
from deeplens.utils.testing_utils import get_size
from experiments.environ import logrecord

from deeplens.full_manager.full_manager import *
from deeplens.dataflow.agg import *
from deeplens.tracking.contour import *
from deeplens.tracking.event import *
from deeplens.simple_manager.manager import *


def runFull(src, cache=False, cleanUp=True, limit=6000, optimizer=True):
    if cleanUp:
        if os.path.exists('/tmp/videos'):
            shutil.rmtree('/tmp/videos')

    manager = FullStorageManager(CustomTagger(FixedCameraBGFGSegmenter().segment, batch_size=30), CropSplitter(),
                                 '/tmp/videos')
    manager.put(src, 'test',
                args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': limit, 'batch_size': 30,
                      'num_processes': 4, 'background_scale': 1}, hwang=False)
    if cache:
        manager.cache('test', Condition(label='foreground'), hwang=False)

    clips = manager.get('test', Condition(label='foreground'))

    region = Box(200, 550, 350, 750)

    pipelines = []
    d = DeepLensOptimizer()
    for c in clips:
        pipeline = c[KeyPoints()][ActivityMetric('one', region)][
            Filter('one', [-0.25, -0.25, 1, -0.25, -0.25], 1.5, delay=10)]
        if optimizer:
            pipeline = d.optimize(pipeline)
        pipelines.append(pipeline)

    result = counts(pipelines, ['one'], stats=True)
    logrecord('naive', ({'size': limit, 'cache': cache, 'optimizer': optimizer, 'file': src,
                         'folder_size': get_size('/tmp/videos')}), 'get', str(result), 's')
    if cache:
        manager.uncache('test', Condition(label='foreground'))

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Enter filename as argv[1]")
        exit(1)
    filename = sys.argv[1]
    limit_list = [150, 300, 600, 1200, 2400, 4800]
    for limit in limit_list:
        runFull(filename, cache=False, limit=limit, optimizer=False)
        runFull(filename, cache=False, limit=limit, optimizer=True)
        runFull(filename, cache=True, limit=limit, optimizer=False)
        runFull(filename, cache=True, limit=limit, optimizer=True)