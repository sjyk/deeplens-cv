import logging
import sys
from multiprocessing.pool import Pool

from deeplens.struct import *
from deeplens.utils import *
from deeplens.dataflow.map import *
from deeplens.dataflow.agg import *
from deeplens.tracking.contour import *
from deeplens.tracking.event import *
from deeplens.object_detection.detect import *
from timeit import default_timer as timer


def boxes_to_labels(frame):
    # print('Frame #' + str(frame['frame']) + ': ' + str(frame['bounding_boxes']))
    sys.stdout.flush()
    labels_set = set()
    for item in frame['bounding_boxes']:
        labels_set.add(item[0])
    return (frame['frame'], labels_set)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    start = timer()
    sys.setrecursionlimit(100000)

    if len(sys.argv) < 2:
        print("Enter filename as argv[1]")
        exit(1)
    filename = sys.argv[1]
    v = VideoStream(filename, limit=10000)
    pipeline = v[Crop(100, 450, 450, 950)][SampleClip(3, 1.0/2)][
        TensorFlowObjectDetect(model_file='ssd_mobilenet_v1_coco_2017_11_17', label_file='mscoco_label_map.pbtxt',
                               num_classes=90, confidence=0.8)]

    labels = [boxes_to_labels(frame) for frame in pipeline]
    print(labels)
    print(labels_to_intervals([label_tuple[1] for label_tuple in labels]))

    end = timer()
    print(end - start)
