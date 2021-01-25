"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

splitters.py defines the main split algorithms used in deeplens's partition.
"""

from deeplens.dataflow.xform import *
from deeplens.streams import *
from deeplens.utils.box import Box
import copy
from deeplens.full_manager.full_manager import *
from deeplens.full_manager.full_video_processing import Splitter
from deeplens.utils.testing_utils import get_size
from deeplens.utils.testing_utils import printCrops
from datetime import datetime
import time

import numpy as np

class SplitterAg2(Splitter):
    def __init__(self, k, c):
        super().__init__()
        self.k = k
        self.c = c

    def _cost(self, hot_spots, crop):
        hot_area = 0
        for hot_spot in hot_spots:
            hot_area += hot_spot.area()
        
        cost = 1 - float(hot_area)/(crop.area*len(hot_spots))
        return cost

    def _mben(self, hs1, hs2, bbs_v, bbs_h):
        v1 = min(hs1[0], hs2[0])
        v2 = max(hs1[0], hs2[0])
        h1 = min(hs1[1], hs2[1])
        h2 = max(hs1[1], hs2[1])

        h = set(bbs_h[h1:h2 + 1])
        v = set(bbs_v[v1:v2 + 1])
        hs = h.intersection(v)
        return hs

    def _cost(self, mben, crop):
        hs_area = 0
        for h in mben:
            hs_area += h.area()
        hs_num = len(mben)
        area = crop.area()
        cost = 1 - (hs_area)/(hs_num*area) #double check this
        return cost

    def initialize(self, data):
        """
        returns (crops, temp_data, do_join)
        """
        #print(data)
        crops = self.map(data)
        #printCrops(crops[0])
        return crops, crops, False

    def map(self, data, test = False):
        """
        Algorithm 2 -> greedy merge of all possible boxes
        """ 
        crops = []
        frame = 0

        bbs = [item['bb'] for sublist in data for item in sublist]
        rem = len(bbs)(len(bbs) - 1)/2

        bbs = [(item, i) for item, i in enumerate(bbs)]

        bbs_v = sorted(bbs, key= lambda x: x[0].x0)
        bbs_h = sorted(bbs, key= lambda x: x[0].y0)

        for i in range(self.k, 0, -1):
            max_crop = None
            max_mben = None
            max_gain = -1
            #check both lists are equal
            assert len(bbs_v) == len(bbs_h)
            for i in range(len(bbs_v)):
                for j in range(i + 1, len(bbs_v)):
                    hs1 = bbs[i]
                    hs2 = bbs[j]
                    x0 = min(hs1[0].x[0], hs2[0].x[0])
                    x1 = max(hs1[0].x[1], hs2[0].x[1])

                    y0 = min(hs1[0].y[0], hs2[0].y[0])
                    y1 = max(hs1[0].y[1], hs2[0].y[1])

                    crop = Box(x0,y0, x1, y1)
                    mben = self._mben(hs1, hs2, bbs_v, bbs_h)
                    mbenl = len(mben)
                    if mbenl < rem/i:
                        cost = self._cost(mben, crop)
                        if cost > self.c:
                            pass
                        mgain = mben/cost
                        if max_gain < mgain:
                            max_crop = None
                            max_gain = mgain
                            max_mben = mben
            
            if max_crop == None:
                # DEAL WITH THIS CASE
                return None

            crops.append(crop)
            rem = rem - len(max_mben)
            if rem <= 0:
                break

            indices = {bb[1] for bb in mben}
            for i in bbs_h:
                if bbs_h[i][1] in indices:
                    del bbs_h[i]
            
            for i in bbs_v:
                if bbs_v[i][1] in indices:
                    del bbs_v[i]         
 
        return crops
    
    def join(self, map1, map2):
        crop2 = map2
        if len(crop2) == 0:
            return (crop2, crop2, True)
        return (crop2, crop2, False)
