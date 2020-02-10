"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

full_video_processing.py defines a video processing splitter that the
storage manager require as input.
"""
IOU_THRESHOLD = 0.7
TRANSLATION_ERROR = 0.05
from deeplens.struct import *

""" Defines a map join operation
"""
class MapJoin():
    def map(self, data):
        """
        Maps a batch of data to an intermediate data structure (temp)
        """
        raise NotImplemented("MapJoin must implement a map function")
    def join(self, map1, map2):
        """
        Returns the final output given the temp of current batch, and the temp
        of pervious batch 
        """
        raise NotImplemented("MapJoin must implement a join function")
    def initialize(self, data):
        raise NotImplemented("MapJoin must implement an initialize function")

""" Defines a video splitter operation
"""
class Splitter():
    def __init__(self):
        self.map_to_video = False
        super().__init__()
    def map(self):
        """
        Maps a batch of data to an intermediate data structure (temp)
        """
        raise NotImplemented("MapJoin must implement a map function")
    def join(self):
        """
        Returns the final output given the temp of current batch, and the temp
        of pervious batch 
        """
        raise NotImplemented("MapJoin must implement a join function")
    def initialize(self):
        raise NotImplemented("MapJoin must implement an initialize function")

""" Creates a crop across different frames
    data: bounding boxes across different frames
"""
class CropSplitter(MapJoin):
    def __init__(self, other_func = None):
        super().__init__()
        self.map_to_video = True

    def initialize(self, data):
        """
        returns (crops, temp_data)
        """
        crops, labels = self.map(data)
        return crops, (crops, labels)

    def map(self, data):
        """
        Union bounding boxes to form crops for a batch of frames
        data: bounding boxes per frame

        returns temp_data
        """
        crops = []
        labels = {}
        index = 0
        frame = 0
        for objects in data:
            for object in objects:
                match = None
                if object['label'] in labels:
                    for i in labels[object['label']]:
                        intersect = float(object['bb'].intersect_area(crops[i]['bb']))
                        union = float(object['bb'].union_area(crops[i]['bb']))
                        iou = intersect/union
                        if iou > IOU_THRESHOLD:
                            match = i
                            break
                    if match:
                        crops[match]['bb'] = crops[match]['bb'].union_box(object['bb'])
                        crops[i]['all'][frame] = object
                        
                if not match:
                    if object['label'] in labels:
                        labels[object['label']].append(index)
                    else:
                        labels[object['label']] = [index]
                    all = {}
                    all[frame] = object
                    crops.append({'bb': object['bb'], 'label': object['label'], 'all': all})
                    index += 1
            frame += 1
        return (crops, labels)

    def join(self, map1, map2):
        """
        Join the second map to the first if it has the objects, and almost
        the same crop sizes.
        Returns: (crop, temp_data, join_prev)
        """
        crop1, labels1 = map1
        crop2, labels2 = map2
        # If the two batches have different number of crops or labels
        # we don't join the crops
        if len(crop1) == 0 and len(crop2) == 0:
            return (crop2, map2, False)
        if len(crop1) != len(crop2):
            return (crop2, map2, False)
        if len(labels1) != len(labels2):
            return (crop2, map2, False)

        crops = [None] * len(crop2)
        indices = 0
        for label in labels2:
            # if the batches have different labels or different number of crops
            # per label, we don't join the crops
            if label not in labels1:
                return (crop2, map2, False)
            if len(labels1[label]) != len(labels2[label]):
                return (crop2, map2, False)
            temp1 = labels2[label]
            for i in labels1[label]:
                remove = -1
                for j in temp1:
                    bb1 = crop1[i]['bb']
                    bb2 = crop2[j]['bb']
                    # Check for interaction between boxes of the same label
                    intersect = float(bb1.intersect_area(bb2))
                    union = float(bb1.union_area(bb2))
                    iou = intersect/union
                    # Check that the boxes are very close to the same size
                    x_diff = abs(bb1.x1 - bb1.x0 - (bb2.x1 - bb2.x0))
                    x_diff = x_diff/float(bb1.x1 - bb1.x0)
                    y_diff = abs(bb1.y1 - bb1.y0 - (bb2.y1 - bb2.y0))
                    y_diff = y_diff/float(bb1.y1 - bb1.y0)
                    # If all conditions are met, the boxes are joined (with translation)
                    if iou > IOU_THRESHOLD and x_diff < TRANSLATION_ERROR and y_diff < TRANSLATION_ERROR:
                        bb =  bb1.x_translate(bb2.x0 - bb1.x0)
                        bb = bb.y_translate(bb2.y0 - bb1.y0)
                        if self.other_func:
                            other = self.other_func(crop1[i]['other'], crop2[j]['other'])
                        else:
                            other = crop1[i]['other'].update(crop2[j]['other'])
                        crops.append({'bb':object['bb'], 'label': object['label'], 'other': other})
                        remove = j
                        break
                if remove == -1:
                    return (crop2, map2, False) # we couldn't find a matching box with the above condition
                del temp1[j]
        return crops, (crops, labels1), True