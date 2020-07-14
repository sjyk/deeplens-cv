import sys

import json
from deeplens.dataflow.xform import *
from deeplens.streams import *
from deeplens.utils.box import Box
import copy
from deeplens.full_manager.full_manager import *
from deeplens.full_manager.full_video_processing import Splitter
from deeplens.utils.testing_utils import get_size
from deeplens.utils.testing_utils import printCrops
from datetime import datetime

def miris_tagger(streams, batch_size):
    bb_labels = JSONListStream(None, 'map_labels', 'car_tracking')
    for i in range(batch_size):
        try:
            curr = next(streams['tracking']).get()
        except StopIteration:
            return bb_labels
        if curr is None:
            JSONListStream.append([], bb_labels)
        else:
            bbs = []
            for lb in curr:
                bb = Box(lb['left']//2, lb['top']//2, lb['right']//2, lb['bottom']//2)
                bbs.append({'bb': bb, 'label': lb['track_id']})
            JSONListStream.append(bbs, bb_labels)
    return bb_labels

#supports labels per crop
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
        #printCrops(crops[0])
        return crops, (crops, labels), False

    def map(self, frames, test = False):
        """
        Union bounding boxes to form crops for a batch of frames
        data: bounding boxes per frame

        returns temp_data
        """ 
        crops = []
        labels = {}
        crop_labels = {}
        frame = 0
        for obs in frames:
            objects = obs.get()
            #if test:
            for ob in objects:
                if 'label' not in ob:
                    continue
                if ob['bb'].x1 - ob['bb'].x0 < 10 or ob['bb'].y1 - ob['bb'].y0 < 10:
                    continue
                if ob['label'] in labels:
                    i = labels[ob['label']]
                    crops[i]['bb'] = crops[i]['bb'].union_box(ob['bb'])
                    crops[i]['all'][frame] = ob
                    crop_labels[ob['label']][i].add(ob['bb'].serialize(), frame)
                else:
                    i = len(crops)
                    labels[ob['label']] = i
                    all_data = {}
                    all_data[frame] = ob
                    crops.append({'bb': ob['bb'], 'label': ob['label'], 'all': all_data})
                    crop_labels[ob['label']] = {i: JSONDictStream(None, str(ob['label']), obs.type, limit = obs.size())}
                    crop_labels[ob['label']][i].add(ob['bb'].serialize(), frame)
            frame += 1
        return (crops, crop_labels), labels

    def join(self, map1, map2):
        """
        Join the second map to the first if it has the objects, and almost
        the same crop sizes.
        Returns: (crop, temp_data, join_prev)
        """
        (crop1, crop_labels1), labels1 = map1
        (crop2, crop_labels2), labels2 = map2
        # If the two batches have different number of crops or labels
        # we don't join the crops
        if len(crop1) == 0 and len(crop2) == 0:
            return ((crop2, crop_labels2), map2, True)
        if len(crop1) != len(crop2):
            return ((crop2, crop_labels2), map2, False)
        if len(labels1) != len(labels2):
            return ((crop2, crop_labels2), map2, False)

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
                return ((crop2, crop_labels2), map2, False) # we couldn't find a matching box with the above condition

        for lb in crop_labels1:
            cr1 = crop_labels1[lb].keys()[0]
            cr2 = crop_labels2[lb].keys()[0]
            crop_labels1[lb][cr1].update(crop_labels2[lb][cr2])
        return (crops, crop_labels1), ((crops, crop_labels1), labels1), True


#does not support labels per crop
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
        labels = {}
        crop_labels = {}
        for objs in data:
            objects = objs.get()
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
                    if ob['label'] not in crop_labels:
                        crop_labels[ob['label']] = {match: JSONDictStream(None, str(ob['label']), objs.type, limit = objs.size())}
                    if match not in crop_labels[ob['label']]:
                        crop_labels[ob['label']][match] = JSONDictStream(None, str(ob['label']), objs.type, limit = objs.size())
                    crop_labels[ob['label']][i].add(ob['bb'].serialize(), frame)
            
                    crops[match]['bb'] = crops[match]['bb'].union_box(ob['bb'])
                    crops[match]['all'][frame] = ob
                if match == -1:
                    if ob['label'] not in crop_labels:
                        crop_labels[ob['label']] = {len(crops): JSONDictStream(None, str(ob['label']), objs.type, limit = objs.size())}
                    match = len(crops)
                    if match not in crop_labels[ob['label']]:
                        crop_labels[ob['label']][match] = JSONDictStream(None, str(ob['label']), objs.type, limit = objs.size())
                    crop_labels[ob['label']][match].add(ob['bb'].serialize(), frame)
                    all = {}
                    all[frame] = ob
                    crops.append({'bb': ob['bb'], 'label': 'foreground', 'all': all})
            frame += 1
        return crops, crop_labels

    def join(self, map1, map2):
        """
        Join the second map to the first if it has the objects, and almost
        the same crop sizes.
        Returns: (crop, temp_data, join_prev)
        """

        (crop1, crop_labels1) = map1
        (crop2, crop_labels2) = map2
        # If the two batches have different number of crops
        # we don't join the crops
        if len(crop1) == 0 and len(crop2) == 0:
            return (map2, map2, True)
        if len(crop1) != len(crop2):
            return (map2, map2, False)

        crops = [None] * len(crop2)
        indices = list(range(len(crop2)))
        crop_labels = {}
        crop_corr1 = [None] * len(crop2)
        crop_corr2 = [None] * len(crop2)
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
                    crops[i] = {'bb':bb, 'label': 'box', 'all': None}
                    remove = j
                    break
            if remove == -1:
                return (map2, map2, False)
            crop_corr1[i] = remove
            crop_corr2[remove] = i
            del indices[remove]
        
        #update labels
        limit = 0
        for lb in crop_labels1:
            for cr in crop_labels1[lb]:
                cr2 = crop_corr1[cr]
                if cr2 in crop_labels2[lb]:
                    limit = crop_labels1[lb][cr].limit + crop_labels2[lb][cr2].limit
                    crop_labels1[lb][cr].updata(crop_labels2[lb][cr2])
                    crop_labels1[lb][cr].update_limit(limit)


        for lb in crop_labels2:
            if lb not in crop_labels1:
                crop_labels1[lb] = {}
            for cr in crop_labels2[lb]:
                cr2 = crop_corr2[cr]
                if cr2 not in crop_labels1[lb]:
                    crop_labels1[lb][cr2] = crop_labels2[lb][cr]
                    crop_labels1[lb][cr2].update_limit(limit)
        
        return (crops, crop_labels1), (crops, crop_labels1), True

class CropAreaSplitter(AreaSplitter):
    def __init__(self, num_crops, iou = 0.2, tran = 0.05):
        super().__init__()
        self.map_to_video = False
        self.iou = iou
        self.tran = 0.05
        self.num_crops = num_crops
    
    def initialize(self, data):
        """
        returns (crops, temp_data)
        """
        crops = self.map(data)
        return crops, crops, False 

    def _pair_iou(self, crops, merged_crops):
        pair_iou = []
        size = len(crops)
        for i in range(size):
            pair_iou.append([])
            for j in range(i + 1, size, 1):
                if merged_crops[i] == -1 or merged_crops[j] == -1:
                    pair_iou[i].append(-1)
                else:
                    intersect = float(crops[j]['bb'].intersect_area(crops[i]['bb']))
                    union = float(crops[j]['bb'].union_area(crops[i]['bb']))
                    iou = intersect/union
                    pair_iou[i].append(iou)
        return pair_iou

    def _max_iou(self, pair_iou):
        max_iou = 0
        pair = (None, None)
        for i in range(len(pair_iou)):
            for j in range(len(pair_iou[i])):
                if max_iou < pair_iou[i][j]:
                    max_iou = pair_iou[i][j]
                    pair = (i, i + j + 1)
        return pair

    def _min_dist(self, crops, merged_crops, max_dist = float('inf')):
        size = len(crops)
        min_dist = max_dist
        pair = (None, None)
        for i in range(size):
            for j in range(i + 1, size, 1):
                if merged_crops[i] == -1 or merged_crops[j] == -1:
                    continue
                dist = crops[i]['bb'].distance(crops[j]['bb'])
                if dist < min_dist:
                     pair = (i, j)
                     min_dist = dist
        return pair

    def map(self, data):
        """
        Union bounding boxes to form crops for a batch of frames
        data: bounding boxes per frame

        returns temp_data
        """ 
        crops = []
        frame = 0
        labels = {}
        crop_labels = {}
        for objs in data:
            objects = objs.get()
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
                    if ob['label'] not in crop_labels:
                        crop_labels[ob['label']] = {match: JSONDictStream(None, str(ob['label']), objs.type, limit = objs.size())}
                    if match not in crop_labels[ob['label']]:
                        crop_labels[ob['label']][match] = JSONDictStream(None, str(ob['label']), objs.type, limit = objs.size())
                    crop_labels[ob['label']][i].add(ob['bb'].serialize(), frame)
            
                    crops[match]['bb'] = crops[match]['bb'].union_box(ob['bb'])
                if match == -1:
                    if ob['label'] not in crop_labels:
                        crop_labels[ob['label']] = {len(crops): JSONDictStream(None, str(ob['label']), objs.type, limit = objs.size())}
                    match = len(crops)
                    if match not in crop_labels[ob['label']]:
                        crop_labels[ob['label']][match] = JSONDictStream(None, str(ob['label']), objs.type, limit = objs.size())
                    crop_labels[ob['label']][match].add(ob['bb'].serialize(), frame)
                    crops.append({'bb': ob['bb'], 'label': 'foreground', 'all': None})
            frame += 1
        
        if len(crops) > self.num_crops:
            merged_crops = [None]*len(crops)
            for i in range(len(crops) - self.num_crops):
                pair_iou = self._pair_iou(crops, merged_crops)
                i, j = self._max_iou(pair_iou)
                # account for zero
                if i is None:
                    print('merged non-overlapping boxes')
                    i, j = self._min_dist(crops, merged_crops)
                crop1 = crops[i]
                crop2 = crops[j]
                bb = crop1['bb'].union_box(crop2['bb'])
                crops.append({'bb': bb, 'label': 'foreground', 'all':None})
                if merged_crops[i] is not None and merged_crops[j] is not None:
                    merged_crops.append((*merged_crops[i], *merged_crops[j]))
                elif merged_crops[i] is not None:
                    merged_crops.append((*merged_crops[i], j))
                elif merged_crops[j] is not None:
                    merged_crops.append((i, *merged_crops[j]))
                else:
                    merged_crops.append((i, j))
                merged_crops[i] = -1
                merged_crops[j] = -1

            new_crop_labels = {}
            for label in crop_labels:
                delete_count = 0
                new_crop_labels[label] = {}
                for i in range(len(merged_crops)):
                    if merged_crops[i] == -1:
                        delete_count += 1
                    elif merged_crops[i] == None:
                        if i in crop_labels[label]:
                            new_crop_labels[label][i - delete_count] = crop_labels[label][i]
                    else:
                        index = i - delete_count
                        for j in merged_crops[i]:
                            if j in crop_labels[label]:
                                old_labels = crop_labels[label][j]
                                if index not in new_crop_labels[label]:
                                    new_crop_labels[label][index] = JSONDictStream(None, label, old_labels.type, limit = old_labels.limit)
                                    # need to update the limit during join !!
                                new_crop_labels[label][i - delete_count].update(crop_labels[label][j])
            new_crops = []
            for i, crop in enumerate(crops):
                if merged_crops[i] != -1:
                    new_crops.append(crops[i])
        
        return new_crops, new_crop_labels


def logrecord(baseline,settings,operation,measurement,*args):
    print(';'.join([baseline, json.dumps(settings), operation, measurement] + list(args)))


# We can directly use a JSONListStream to 
def main(video, json_labels):
    labels = {'tracking': JSONListStream(json_labels, 'tracking', 'labels', isList = True)}
    manager = FullStorageManager(miris_tagger, CropAreaSplitter(4), 'miris4')
    manager.put(video, 'test0', aux_streams = labels)

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Enter video filepath as argv[1], JSON filepath as argv[2]")
        exit(1)
    video = sys.argv[1]
    json_labels = sys.argv[2]
    main(video, json_labels)

