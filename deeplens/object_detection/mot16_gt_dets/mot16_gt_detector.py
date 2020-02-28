#  DeepLens
#  Copyright (c) 2019. Adam Dziedzic and Sanjay Krishnan
#  Licensed under The MIT License [see LICENSE for details]
#  Written by Adam Dziedzic

from object_detection.interface_detector import Detector
import numpy as np

class Mot16GtDetector(Detector):
    def __init__(self, args):
        input_path = args.mot16_gt_dets + args.bench_case + "/det/det.txt"
        self.raw = np.genfromtxt(input_path, delimiter=',', dtype=np.float32)
        self.raw[:, 4:6] += self.raw[:, 2:4]  # x1, y1, w, h -> x1, y1, x2, y2
        self.end_frame = int(np.max(self.raw[:, 0]))
        self.frame_num = 0


    def detect(self, _):
        """
        Implement the interface from the Detector abstract class.

        :param img: the input image
        :return: detections, each in the following format:
        (x1, y1, x2, y2, conf_level, class_pred)
        """
        self.frame_num += 1
        assert self.frame_num <= self.end_frame
        idx = self.raw[:, 0] == self.frame_num
        return self.raw[idx, 2:8]


