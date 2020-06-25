import sys

from deeplens.simple_manager.manager import *

import json


from deeplens.dataflow.xform import *
from deeplens.utils.ui import play, overlay

if len(sys.argv) < 3:
    print("Enter video filepath as argv[1], JSON filepath as argv[2]")
    exit(1)
v = VideoStream(sys.argv[1])
with open(sys.argv[2], 'r') as json_file:
    labels = json.loads(json_file.read())

pipeline = v

for p in pipeline:
    image = p['data']
    if labels[p['frame']] == None:
        continue
    bbox = [(str(label['track_id']), (label['left']//2, label['top']//2, label['right']//2, label['bottom']//2)) for label in labels[p['frame']]]
    image = overlay(image, bbox)
    #print(p['bounding_boxes'])
    #hit q to exit
    cv2.imshow('Player',image)

    #print("test")

    if cv2.waitKey(1) & 0xFF == ord('q'):
        exit()
