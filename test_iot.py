import sys

from deeplens.dataflow.map import Crop
from deeplens.struct import VideoStream
from deeplens.tracking.contour import *
from deeplens.utils.utils import overlay

src = sys.argv[1]
c = VideoStream(src, limit=-1)
pipeline = c[Crop(372, 122, 572, 445)][KeyPoints(blur=5, edge_low=500, edge_high=600, area_thresh=10, label="object")]

count = []
for frame in pipeline:
    if len(frame['bounding_boxes']) >= 1:
        # print((frame['frame'], len(frame['bounding_boxes'])))
        count.append((frame['frame'], len(frame['bounding_boxes'])))

    img = overlay(frame['data'], frame['bounding_boxes'])
    cv2.imshow('Player', img)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        exit()

print(count)
