import sys

import json
from deeplens.dataflow.xform import *
from deeplens.streams import *
from deeplens.utils.box import Box
import copy
from deeplens.full_manager.full_manager import *
from deeplens.full_manager.full_video_processing import Splitter
from deeplens.utils.testing_utils import printCrops

def miris_tagger(labels, batch_size):
    bb_labels = []
    for i in range(batch_size):
        curr = next(labels).get()
        if curr is None:
            bb_labels.append([])
        else:
            bbs = []
            for lb in curr:
                bb = Box(lb['left']//2, lb['top']//2, lb['right']//2, lb['bottom']//2)
                bbs.append({'bb': bb, 'label': lb['track_id']})
            bb_labels.append(bbs)
    
    return bb_labels 

class TrackSplitter(Splitter):
    def __init__(self, iou = 0.7, tran = 0.05):
        super().__init__()
        self.map_to_video = False
        self.iou = iou
        self.tran = tran
    
    def initialize(self, data):
        """
        returns (crops, temp_data)
        """
        #print(data)
        crops, labels = self.map(data)
        #printCrops(crops)
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
            #   print(objects)
            for ob in objects:
                if 'label' not in ob:
                    continue
                if ob['bb'].x1 - ob['bb'].x0 < 10 or ob['bb'].y1 - ob['bb'].y0 < 10:
                    continue
                if ob['label'] in labels:
                    i = labels[ob['label']]
                    crops[i]['bb'] = crops[i]['bb'].union_box(ob['bb'])
                    crops[i]['all'][frame] = ob
                
                else:
                    i = len(crops)
                    labels[ob['label']] = i
                    all_data = {}
                    all_data[frame] = ob
                    crops.append({'bb': ob['bb'], 'label': ob['label'], 'all': all_data})
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
            return (crop2, map2, True)
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
            j = labels2[label]
            i = labels1[label]
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
            if iou > self.iou and x_diff < self.tran and y_diff < self.tran:
                bb =  bb1.x_translate(bb2.x0 - bb1.x0)
                bb = bb.y_translate(bb2.y0 - bb1.y0)
                crop1[i]['all'].update(crop2[j]['all'])
                crops[i] = {'bb':bb, 'label': label, 'all': crop1[i]['all']}
            else:
                return (crop2, map2, False) # we couldn't find a matching box with the above condition

        return crops, (crops, labels1), True


class AreaSplitter(Splitter):
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
        for objects in data:
            for ob in objects:
                match = -1
                if 'label' not in ob:
                    continue
                if ob['bb'].x1 - ob['bb'].x0 < 10 or ob['bb'].y1 - ob['bb'].y0 < 10:
                    continue
                for i in range(len(crops)):
                    intersect = float(ob['bb'].intersect_area(crops[i]['bb']))
                    union = float(ob['bb'].union_area(crops[i]['bb']))
                    iou = intersect/union
                    if iou > self.iou:
                        match = i
                        break
                if match != -1:
                    crops[match]['bb'] = crops[match]['bb'].union_box(ob['bb'])
                    crops[match]['all'][frame] = ob
                if match == -1:
                    all = {}
                    all[frame] = ob
                    crops.append({'bb': ob['bb'], 'label': 'box', 'all': all})
            frame += 1
        return crops

    def join(self, crop1, crop2):
        """
        Join the second map to the first if it has the objects, and almost
        the same crop sizes.
        Returns: (crop, temp_data, join_prev)
        """
        # If the two batches have different number of crops
        # we don't join the crops
        if len(crop1) == 0 and len(crop2) == 0:
            return (crop2, crop2, True)
        if len(crop1) != len(crop2):
            return (crop2, crop2, False)

        crops = [None] * len(crop2)
        indices = list(range(len(crop2)))

        for i in range(len(crop1)):
            
            bb1 = crop1[i]['bb']
            remove = -1
            for j in range(len(indices)):
                # Check for interaction between boxes of the same label
                bb2 = crop2[j]['bb']
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
                    crop1[i]['all'].update(crop2[j]['all'])
                    crops[i] = {'bb':bb, 'label': 'box', 'all': crop1[i]['all']}
                    remove = j
                    break
            if remove == -1:
                    return (crop2, crop2, False)
            del indices[remove]

        return (crops, crops, True)

# We can directly use a JSONListStream to 
def main(video, json_labels):
    labels = {'labels': JSONListStream(json_labels, 'labels', True)}
    manager = FullStorageManager(miris_tagger, TrackSplitter(), 'miris')
    manager.put(video, 'test0', aux_streams = labels)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Enter video filepath as argv[1], JSON filepath as argv[2]")
        exit(1)
    video = sys.argv[1]
    json_labels = sys.argv[2]
    main(video, json_labels)

