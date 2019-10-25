import sys
from multiprocessing.pool import Pool

from dlcv.struct import *
from dlcv.utils import *
from dlcv.dataflow.map import *
from dlcv.dataflow.agg import *
from dlcv.tracking.contour import *
from dlcv.tracking.event import *
from dlcv.object_detection.detect import *


def boxes_to_labels(frame):
    print('Frame #' + str(frame['frame']) + ': ' + str(frame['bounding_boxes']))
    sys.stdout.flush()
    labels = set()
    for item in frame['bounding_boxes']:
        labels.add(item[0])
    return labels


if __name__ == '__main__':
    v = VideoStream('cut.mp4', limit=10)
    pipeline = v[Crop(100, 450, 450, 950)][
        TensorFlowObjectDetect(model_file='ssd_mobilenet_v1_coco_2017_11_17', label_file='mscoco_label_map.pbtxt',
                               num_classes=90, confidence=0.2)]

    labels = [boxes_to_labels(frame) for frame in pipeline]
    print(labels_to_intervals(labels))
