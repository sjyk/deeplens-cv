
from deeplens.full_manager.full_video_processing import CropUnionSplitter
from deeplens.video.tracking.background import FixedCameraBGFGSegmenter
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *
from deeplens.streams import *
from deeplens.utils.box import *
from deeplens.utils.utils import *
import matplotlib.pyplot as plt
import sys
import cv2


manager = FullStorageManager(FixedCameraBGFGSegmenter().segment, CropUnionSplitter(), 'videos2')
crop = {'bb': Box(0, 0, 1000, 1000), 'label': 'background'}
crops = {'crops': ConstantStream(([crop], False), 'crops')}

#label = [{'label': 'test', 'bb': Box(0, 1010, 0, 1010), 'frame': i/60.0} for i in range(120)]
#labels = {'label': JSONListStream(label, 'label', ttype='time')}
manager.put('./videos/cut4.mp4', 'test2')
# res = manager.get("SELECT * FROM clip WHERE video_name = '%s'" %('test2'))
# print(res)
# #print([video for video in res[0]][0]['data'].shape)
# #print(len(res))
# #play(res[0])

# Usage: python3 test_basic.py video1.mp4 video2.mp4 video3.mp4 ...
# streams = sys.argv[1:]

# limit = 5000

# start_0 = time.time()
# vstream = CVVideoStreams(streams, 'test', test_limit=limit) #test_limit defines the batch size here
# #vstream = cv2.VideoCapture('videos/cut2.mp4')
# latency = []
# now = time.time()
# for frame in vstream:
#     if start_0:
#         start = start_0
#         start_0 = 0
#     f = frame.get()
#     end = time.time()
#     latency.append(end - start)
#     start = end

# print("Total time:", time.time() - now)

# plt.plot(latency)
# plt.show()