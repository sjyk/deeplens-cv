import sys

from deeplens.dataflow.agg import count
from deeplens.full_manager.full_manager import FullStorageManager
from deeplens.optimizer.deeplens import DeepLensOptimizer
from deeplens.struct import CustomTagger, VideoStream, Box
from deeplens.tracking.contour import KeyPoints
from deeplens.tracking.event import Filter, ActivityMetric

if len(sys.argv) < 2:
    print("Enter filename as argv[1]")
    exit(1)
filename = sys.argv[1]

vs = VideoStream(filename, hwang=True, rows=range(0,8000,400))

region = Box(200, 550, 350, 750)

d = DeepLensOptimizer()
pipeline = vs[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25, -0.25, 1, -0.25, -0.25], 1.5, delay=10)]
# pipeline = d.optimize(pipeline)

result = count(pipeline, ['one'], stats=True)
print("Hwang:", result)

vs = VideoStream(filename, hwang=False, limit=500)

region = Box(200, 550, 350, 750)

d = DeepLensOptimizer()
pipeline = vs[KeyPoints()][ActivityMetric('one', region)][Filter('one', [-0.25, -0.25, 1, -0.25, -0.25], 1.5, delay=10)]
# pipeline = d.optimize(pipeline)

result = count(pipeline, ['one'], stats=True)
print("OpenCV:", result)
