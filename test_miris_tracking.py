import os
import sys
import json
import cv2

from timeit import default_timer as timer
from deeplens.constants import *
from deeplens.struct import VideoStream
from deeplens.utils.frame_xform import *
from deeplens.utils.ui import play, overlay
from experiments.environ import logrecord

if len(sys.argv) < 3:
    print("Enter video filepath as argv[1], JSON filepath as argv[2]")
    exit(1)
v = VideoStream(sys.argv[1])
with open(sys.argv[2], 'r') as json_file:
    labels = json.loads(json_file.read())

pipeline = v
frame_rate = 10
writers = {}
width = {}
height = {}
filenames = {}
fourcc = cv2.VideoWriter_fourcc(*MP4V)
folder = '/tmp/miris/'

now = timer()
for i in pipeline:
    pass

logrecord('naive', ({'folder_size': os.path.getsize(sys.argv[1]) }), 'get', str(timer() - now), 's')

now = timer()
for p in pipeline:
    image = p['data']
    if labels[p['frame']] == None:
        continue
    bboxes = [(str(label['track_id']), (label['left']//2, label['top']//2, label['right']//2, label['bottom']//2)) for label in labels[p['frame']]]

    for id, bbox in bboxes:
        filenames[id] = folder + str(id) + '.avi'
        if id not in writers:
            width[id] = bbox[2] - bbox[0]
            height[id] = bbox[3] - bbox[1]
            writers[id] = cv2.VideoWriter(filenames[id],
                                          fourcc,
                                          frame_rate,
                                          (width[id], height[id]),
                                          True)
            writers[id].write(crop_box(image, Box(*bbox)))
        else:
            writers[id].write(cv2.resize(crop_box(image, Box(*bbox)), (width[id], height[id])))

for writer in writers.values():
    writer.release()

logrecord('fullTrack', ({'folder_size': os.path.getsize(folder) }), 'put', str(timer() - now), 's')

# measure the latency of get()
now = timer()
for clip in filenames.values():
    c = VideoStream(clip)
    for i in c:
        pass

logrecord('fullTrack', ({'folder_size': os.path.getsize(folder) }), 'get', str(timer() - now), 's')