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

id_dict = {}

for p in pipeline:
    image = p['data']
    if labels[p['frame']] == None:
        continue
    bboxes = [(str(label['track_id']), (label['left']//2, label['top']//2, label['right']//2, label['bottom']//2)) for label in labels[p['frame']]]
    for id, bbox in bboxes:
        if id not in id_dict:
            id_dict[id] = bbox
        else:
            id_dict[id] = (min(bbox[0], id_dict[id][0]), min(bbox[1], id_dict[id][1]),
                           max(bbox[2], id_dict[id][2]), max(bbox[3], id_dict[id][3]))
    bboxes_this_frame = [(id, id_dict[id]) for id, _ in bboxes]
    image = overlay(image, bboxes_this_frame)
    #print(p['bounding_boxes'])
    #hit q to exit
    cv2.imshow('Player',image)

    #print("test")

    if cv2.waitKey(1) & 0xFF == ord('q'):
        exit()
