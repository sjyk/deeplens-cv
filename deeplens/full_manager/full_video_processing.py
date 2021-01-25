"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

full_video_processing.py defines a video processing splitter that the
storage manager require as input.
"""
import copy
import logging

IOU_THRESHOLD = 0.7
TRANSLATION_ERROR = 0.05

""" Defines a map join operation
"""
class MapJoin():
    def map(self, data):
        """
        Maps a batch of data to an intermediate data structure (temp)
        """
        raise NotImplementedError("MapJoin must implement a map function")
    def join(self, map1, map2):
        """
        Returns the final output given the temp of current batch, and the temp
        of previous batch
        """
        raise NotImplementedError("MapJoin must implement a join function")
    def initialize(self, data):
        raise NotImplementedError("MapJoin must implement an initialize function")

""" Defines a video splitter operation - Not Used
"""
class Splitter():
    def __init__(self):
        self.map_to_video = False
        super().__init__()
    def map(self):
        """
        Maps a batch of data to an intermediate data structure (temp)
        """
        raise NotImplementedError("MapJoin must implement a map function")
    def join(self):
        """
        Returns the final output given the temp of current batch, and the temp
        of previous batch
        """
        raise NotImplementedError("MapJoin must implement a join function")
    def initialize(self):
        raise NotImplementedError("MapJoin must implement an initialize function")


""" Creates a crop across different frames
    data: bounding boxes across different frames
    NOTE: These splitters need to be re-tested because of API changes?
"""
class IoUSplitter(Splitter):
    def __init__(self, iou = 0.2, tran = 0.05):
        super().__init__()
        self.map_to_video = False
        self.iou = iou
        self.tran = 0.05
    
    def initialize(self, data):
        """
        returns (crops, temp_data)
        """
        crops = self.map(data)
        return crops, crops, False 
    
    def map(self, data):
        """
        Union bounding boxes to form crops for a batch of frames
        data: bounding boxes per frame

        returns temp_data
        """ 
        crops = []
        frame = 0
        for objs in data:
            objects = objs.get()
            for ob in objects:
                match = -1
                if 'label' not in ob:
                    continue
                if ob['bb'].x1 - ob['bb'].x0 < 10 or ob['bb'].y1 - ob['bb'].y0 < 10:
                    continue
                for i in range(len(crops)):
                    intersect = float(ob['bb'].intersect_area(crops[i]))
                    union = float(ob['bb'].union_area(crops[i]))
                    iou = intersect/union
                    if iou > self.iou:
                        match = i
                        break
                if match != -1:            
                    crops[match] = crops[match].union_box(ob['bb'])
                if match == -1:
                    crops.append(ob['bb'])
            frame += 1
        return crops

    def join(self, map1, map2):
        """
        Join the second map to the first if it has the objects, and almost
        the same crop sizes.
        Returns: (crop, temp_data, join_prev)
        """

        crop1 = map1
        crop2 = map2
        # If the two batches have different number of crops
        # we don't join the crops
        if len(crop1) == 0 and len(crop2) == 0:
            return (map2, map2, True)
        if len(crop1) != len(crop2):
            return (map2, map2, False)

        crops = [None] * len(crop2)
        indices = list(range(len(crop2)))
        
        for i in range(len(crop1)):
            bb1 = crop1[i]
            remove = -1
            for j in indices:
                # Check for interaction between boxes of the same label
                bb2 = crop2[j]
                intersect = float(bb1.intersect_area(bb2))
                union = float(bb1.union_area(bb2))
                iou = intersect/union
                # Check that the boxes are very close to the same size
                x_diff = abs(bb1.x1 - bb1.x0 - (bb2.x1 - bb2.x0))
                x_diff = x_diff/float(bb1.x1 - bb1.x0)
                y_diff = abs(bb1.y1 - bb1.y0 - (bb2.y1 - bb2.y0))
                y_diff = y_diff/float(bb1.y1 - bb1.y0)
                if iou > self.iou and x_diff < self.tran and y_diff < self.tran:
                    bb =  bb1.x_translate(bb2.x0 - bb1.x0)
                    bb = bb.y_translate(bb2.y0 - bb1.y0)
                    crops[i] = bb
                    remove = j
                    break
            if remove == -1:
                return (map2, map2, False)
            
            del indices[remove]
        
        return crops, crops, True

""" Creates a crop across different frames
    data: bounding boxes across different frames
"""
class UnionSplitter(Splitter):
    def __init__(self):
        super().__init__()
        self.map_to_video = True

    def initialize(self, data):
        """
        returns (crops, temp_data)
        """
        crops = self.map(data)
        return crops, crops, False

    def map(self, data):
        """
        Union bounding boxes to form crops for a batch of frames
        data: bounding boxes - NOT per frame

        returns temp_data
        """ 
        crops = []
        index = 0
        frame = 0
        num_match = 0
        for object in data:
            if len(crops) == 0:
                crops.append(object['bb'])
            crops[0] = crops[0].union_box(object['bb'])
        return crops

    def join(self, crop1, crop2):
        """
        Join the second map to the first if it has the objects, and almost
        the same crop sizes.
        Returns: (crop, temp_data, join_prev)
        """
        # If the two batches have different number of crops or labels
        # we don't join the crops
        if len(crop1) == 0 and len(crop2) == 0:
            return (crop2, crop2, True)
        if len(crop1) != len(crop2):
            return (crop2, crop2, False)
        crops = [None] * len(crop2)
        remove = False
        
        bb1 = crop1[0]
        bb2 = crop2[0]
        # Check for interaction between boxes of the same label
        intersect = float(bb1.intersect_area(bb2))
        union = float(bb1.union_area(bb2))
        if union != 0:
            iou = intersect/union
        else:
            iou = 0
        # Check that the boxes are very close to the same size
        x_diff = abs(bb1.x1 - bb1.x0 - (bb2.x1 - bb2.x0))
        x_diff = x_diff/float(bb1.x1 - bb1.x0)
        y_diff = abs(bb1.y1 - bb1.y0 - (bb2.y1 - bb2.y0))
        y_diff = y_diff/float(bb1.y1 - bb1.y0)
        # If all conditions are met, the boxes are joined (with translation)
        if iou > IOU_THRESHOLD and x_diff < TRANSLATION_ERROR and y_diff < TRANSLATION_ERROR:
            bb =  bb1.x_translate(bb2.x0 - bb1.x0)
            bb = bb.y_translate(bb2.y0 - bb1.y0)
            crops[0] = bb
            remove = True
        if not remove:
            return (crop2, crop2, False) # we couldn't find a matching box with the above condition
        return crops, crops, True