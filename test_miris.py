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
from miris_splitters import *

def miris_tagger(streams, op_name, batch_size, fframe):
    bb_labels = []
    for i in range(batch_size):
        try:
            curr = streams['tracking'].next(op_name)
        except StopIteration:
            if i != 0:
                return bb_labels
            else:
                raise StopIteration()
        if curr is None:
            bb_labels.append([])
        else:
            bbs = []
            for lb in curr:
                bb = Box(lb['left']//2, lb['top']//2, lb['right']//2, lb['bottom']//2)
                bbs.append({'bb': bb, 'label': lb['track_id'], 'frame': fframe + i})
            bb_labels.append(bbs)
    return bb_labels

def logrecord(baseline,settings,operation,measurement,*args):
    print(';'.join([baseline, json.dumps(settings), operation, measurement] + list(args)))


# We can directly use a JSONListStream to 
def main(video, json_labels):
    labels = {'tracking': JSONListStream(json_labels, 'tracking', is_file = True, is_list = True)}
    manager = FullStorageManager(miris_tagger, AreaTrackSplitter(4), 'miris_test')
    manager.put(video, 'test0', map_streams = labels)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Enter video filepath as argv[1], JSON filepath as argv[2]")
        exit(1)
    video = sys.argv[1]
    json_labels = sys.argv[2]
    main(video, json_labels)

