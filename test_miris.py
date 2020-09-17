import sys

import json
from deeplens.dataflow.xform import *
from deeplens.streams import *
from deeplens.utils.box import Box
import copy
from deeplens.full_manager.full_manager import *
from deeplens.full_manager.full_video_processing import Splitter
from deeplens.utils.testing_utils import get_size
from deeplens.utils.testing_utils import printCrops
from datetime import datetime

def miris_tagger(streams, batch_size, start_frame):
    bb_labels = JSONListStream(None, 'map_labels', 'car_tracking')
    for i in range(batch_size):
        try:
            curr = next(streams['tracking']).get()
        except StopIteration:
            return bb_labels
        if curr is None:
            JSONListStream.append([], bb_labels)
        else:
            bbs = []
            for lb in curr:
                bb = Box(lb['left']//2, lb['top']//2, lb['right']//2, lb['bottom']//2)
                bbs.append({'bb': bb, 'label': lb['track_id'], 'frame': i + start_frame})
            JSONListStream.append(bbs, bb_labels)
    return bb_labels

def logrecord(baseline,settings,operation,measurement,*args):
    print(';'.join([baseline, json.dumps(settings), operation, measurement] + list(args)))


# We can directly use a JSONListStream to 
def main(video, json_labels):
    labels = {'tracking': JSONListStream(json_labels, 'tracking', 'labels', isList = True)}
    manager = FullStorageManager(miris_tagger, CropAreaSplitter(4), 'miris4')
    manager.put(video, 'test0', aux_streams = labels)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Enter video filepath as argv[1], JSON filepath as argv[2]")
        exit(1)
    video = sys.argv[1]
    json_labels = sys.argv[2]
    main(video, json_labels)

