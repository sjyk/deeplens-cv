import sys

from deeplens.struct import *
from deeplens.dataflow.map import *
from deeplens.object_detection.detect import *
from deeplens.dataflow.buffer_reduce import *
from timeit import default_timer as timer


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    start = timer()
    sys.setrecursionlimit(100000)

    if len(sys.argv) < 2:
        print("Enter filename as argv[1]")
        exit(1)
    filename = sys.argv[1]
    v = VideoStream(filename, limit=100)
    pipeline = v[Crop(100, 450, 450, 950)][SampleClip(3, 1.0/2)][
        TensorFlowObjectDetectReduce(model_file='ssd_mobilenet_v1_coco_2017_11_17', label_file='mscoco_label_map.pbtxt',
                                     num_classes=90, confidence=0.8)]

    print([frame for frame in pipeline])


    end = timer()
    print(end - start)
