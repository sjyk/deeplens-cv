import sys
from experiments.environ import *

from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.optimizer.deeplens import DeepLensOptimizer

from deeplens.struct import *
from deeplens.dataflow.map import *
from deeplens.full_manager.full_manager import *
from deeplens.utils.testing_utils import *
from deeplens.dataflow.agg import *
from deeplens.tracking.contour import *
from deeplens.tracking.event import *
from deeplens.core import *
from deeplens.simple_manager.manager import *
import os

import cv2
import numpy as np


from deeplens.dataflow.xform import *
from deeplens.utils.ui import play, overlay
from deeplens.constants import *

#v = VideoStream('/Users/sanjayk/Downloads/traffic-001.mp4', limit=2000)
#v = VideoStream('/Users/sanjayk/Downloads/panoramic-000-001.mp4', limit=2000)
def test_keypoint(video, output):
    v = VideoStream(video, limit = 2000)
    pipeline = v[SizeMovementDetector()][KeyPointFilter()]
    writer = None
    for p in pipeline:
        if writer == None:
            width = v.width
            height = v.height
            fourcc = cv2.VideoWriter_fourcc(*MP4V)
            writer = cv2.VideoWriter(output, fourcc, 5, (width, height), True)
        image = p['data']
        image = overlay(image, p['bounding_boxes'], labelp = False)
        writer.write(image)

        #print(p['bounding_boxes'])
        #hit q to exit
        #cv2.imshow('Player',image)

        #print("test")

        #if cv2.waitKey(1) & 0xFF == ord('q'):
        #    exit()

def main():
    dir = '../experiments/data/warsaw/videos'
    output = './output/experiment'

    count = 0
    for f in os.listdir(dir):
        if f.endswith('mp4'):
            f = os.path.join(dir, f)
            print(f)
            test_keypoint(f, output + str(count) + '.avi')
            print('Finished')
            count += 1



if __name__ == '__main__':
    main()