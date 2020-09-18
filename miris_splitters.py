

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


#supports labels per crop
class TrackSplitter(Splitter):
    def __init__(self, iou = 0.7, tran = 0.05):
        super().__init__()
        self.iou = iou
        self.tran = tran
    
    def initialize(self, data):
        """
        returns (crops, temp_data, do_join)
        """
        #print(data)
        crops, labels = self.map(data)
        #printCrops(crops[0])
        return crops, (crops, labels), False

    def map(self, data, test = False):
        """
        Union bounding boxes to form crops for a batch of frames
        data: bounding boxes per frame

        returns temp_data
        """ 
        crops = []
        labels = {}
        frame = 0
        for objects in data:
            #if test:
            for ob in objects:
                if 'bb' not in ob:
                    continue
                if ob['bb'].x1 - ob['bb'].x0 < 10 or ob['bb'].y1 - ob['bb'].y0 < 10:
                    continue
                if ob['label'] in labels:
                    i = labels[ob['label']]
                    crops[i] = crops[i].union_box(ob['bb'])
                else:
                    i = len(crops)
                    labels[ob['label']] = i
                    crops.append(ob['bb'])
            frame += 1
        return crops, labels
    
    def join(self, map1, map2):
        crop2, labels2 = map2
        return (crop2, map2, False)

    # def join(self, map1, map2):
    #     """
    #     Join the second map to the first if it has the objects, and almost
    #     the same crop sizes.
    #     Returns: (crop, temp_data, join_prev)
    #     """
    #     crop1, labels1 = map1
    #     crop2, labels2 = map2
    #     # If the two batches have different number of crops or labels
    #     # we don't join the crops
    #     if len(crop1) == 0 and len(crop2) == 0:
    #         return (crop2, map2, True)
    #     if len(crop1) != len(crop2):
    #         return (crop2, map2, False)
    #     if len(labels1) != len(labels2):
    #         return (crop2, map2, False)

    #     crops = [None] * len(crop2)
    #     indices = 0
    #     for label in labels2:
    #         # if the batches have different labels or different number of crops
    #         # per label, we don't join the crops
    #         if label not in labels1:
    #             return (crop2, map2, False) 
    #         j = labels2[label]
    #         i = labels1[label]
    #         bb1 = crop1[i]
    #         bb2 = crop2[j]
    #         # Check for interaction between boxes of the same label
    #         intersect = float(bb1.intersect_area(bb2))
    #         union = float(bb1.union_area(bb2))
    #         iou = intersect/union
    #         # Check that the boxes are very close to the same size
    #         x_diff = abs(bb1.x1 - bb1.x0 - (bb2.x1 - bb2.x0))
    #         x_diff = x_diff/float(bb1.x1 - bb1.x0)
    #         y_diff = abs(bb1.y1 - bb1.y0 - (bb2.y1 - bb2.y0))
    #         y_diff = y_diff/float(bb1.y1 - bb1.y0)
    #         # If all conditions are met, the boxes are joined (with translation)
    #         if iou > self.iou and x_diff < self.tran and y_diff < self.tran:
    #             bb =  bb1.x_translate(bb2.x0 - bb1.x0)
    #             bb = bb.y_translate(bb2.y0 - bb1.y0)
    #             crops[i] = bb
    #         else:
    #             return (crop2, map2, False) # we couldn't find a matching box with the above condition


    #     return crops, (crops, labels1), True

class AreaSplitter(Splitter):
    def __init__(self, iou = 0.1):
        super().__init__()
        self.iou = iou
    
    def initialize(self, data):
        """
        returns (crops, temp_data)
        """
        crops = self.map(data)
        return crops, crops, False 

    def _pair_iou(self, crops, pair_iou = None, index = None):
        if pair_iou == None or index:
            pair_iou = {}
            for i in crops:
                pair_iou[i] = {}
                for j in crops:
                    if j >= i:
                        continue
                    intersect = float(crops[j].intersect_area(crops[i]))
                    union = float(crops[j].union_area(crops[i]))
                    iou = intersect/union
                    pair_iou[i][j] = iou
        else:
            # we only need to save once
            pair_iou[index] = {}
            for j in crops:
                if j == index:
                    continue
                intersect = float(crops[j].intersect_area(crops[index]))
                union = float(crops[j].union_area(crops[index]))
                iou = intersect/union
                if j > index:
                    pair_iou[index][j] = iou
                else:
                    pair_iou[j][index] = iou
        return pair_iou

    def _max_iou(self, pair_iou):
        max_iou = 0
        pair = (None, None)
        for i in pair_iou:
            for j in pair_iou[i]:
                if max_iou < pair_iou[i][j]:
                    max_iou = pair_iou[i][j]
                    pair = (i, j)
        if max_iou > self.iou:
            return pair
        else:
            return (None, None)

    def map(self, data):
        """
        Union bounding boxes to form crops for a batch of frames
        data: bounding boxes per frame

        returns temp_data
        """ 
        crops = {}
        index = 0
        pair_iou = None
        for objects in data:
            for ob in objects:
                match = -1
                if 'bb' not in ob:
                    continue
                if ob['bb'].x1 - ob['bb'].x0 < 10 or ob['bb'].y1 - ob['bb'].y0 < 10:
                    continue
                merged = False
                for i in crops:
                    intersect = float(crops[i].intersect_area(ob['bb']))
                    union = float(crops[i].union_area(crops[i]))
                    iou = intersect/union
                    if iou >= 0.7:
                        crops[i] = ob['bb'].union_box(crops[i])
                        merged = True
                        break
                if not merged:
                    crops[index] = ob['bb']
                    index +=1
        
        while True:
            if i == 0:
                pair_iou = self._pair_iou(crops, pair_iou = pair_iou)
            else:
                pair_iou = self._pair_iou(crops, pair_iou = pair_iou, index= index - 1)
            i, j = self._max_iou(pair_iou)
            # account for zero
            if i is None:
                break
            crop1 = crops[i]
            crop2 = crops[j]
            bb = crop1.union_box(crop2)
            del crops[i]
            del crops[j]
            crops[index] = bb
            index += 1
            del pair_iou[i]
            del pair_iou[j]
            
            for a in pair_iou:
                kee = copy.copy(list(pair_iou[a].keys()))
                for b in kee:
                    if b == i or b == j:
                        del pair_iou[a][b]
        cropsl = [crops[i] for i in crops]
        crops = cropsl
        return crops
        
    def join(self, map1, map2):
        return (map2, map2, False)

class AreaTrackSplitter(AreaSplitter):

    def map(self, data):
        """
        Union bounding boxes to form crops for a batch of frames
        data: bounding boxes per frame

        returns temp_data
        """ 
        crops = {}
        labels = {}
        for objects in data:
            #if test:
            for ob in objects:
                if 'bb' not in ob:
                    continue
                if ob['bb'].x1 - ob['bb'].x0 < 10 or ob['bb'].y1 - ob['bb'].y0 < 10:
                    continue
                if ob['label'] in labels:
                    i = labels[ob['label']]
                    crops[i] = crops[i].union_box(ob['bb'])
                else:
                    i = len(crops)
                    labels[ob['label']] = i
                    crops[i] = ob['bb']
        
        pair_iou = None
        index = len(crops)
        while True:
            if i == 0:
                pair_iou = self._pair_iou(crops, pair_iou = pair_iou)
            else:
                pair_iou = self._pair_iou(crops, pair_iou = pair_iou, index= index - 1)
            i, j = self._max_iou(pair_iou)
            # account for zero
            if i is None:
                break
            crop1 = crops[i]
            crop2 = crops[j]
            bb = crop1.union_box(crop2)
            del crops[i]
            del crops[j]
            crops[index] = bb
            index += 1
            del pair_iou[i]
            del pair_iou[j]
            
            for a in pair_iou:
                kee = copy.copy(list(pair_iou[a].keys()))
                for b in kee:
                    if b == i or b == j:
                        del pair_iou[a][b]
        cropsl = [crops[i] for i in crops]
        crops = cropsl

        return crops