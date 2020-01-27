"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

full_video_processing.py defines a video processing splitter that the
storage manager require as input.
"""
IOU_THRESHOLD = 0.7
""" Defines a basic map join operation
"""
class MapJoin():
    def map():
        raise NotImplemented("MapJoin must implement a map function")
    def join():
        raise NotImplemented("MapJoin must implement a join function")


""" Creates a crop across different frames
    data: bounding boxes across different frames
"""
class CropSplitter(MapJoin):

    def map(data):
        """
        Union bounding boxes to form crops for a batch of frames
        data: bounding boxes per frame
        """
        crops = []
        labels = {}
        index = 0
        for objects in data:
            for object in objects:
                if object['label'] in labels:
                    match = -1
                    for i in labels[object['label']]:
                        intersect = float(object['bb'].intersect_area(crop[i]['bb']))
                        union = float(object['bb'].union_area(crop[i]['bb']))
                        iou = intersect/union
                        if iou > IOU_THRESHOLD:
                            match = i
                            break
                    if match != -1:
                        crop[i]['bb'] = crop[i]['bb'].union_box(object['bb'])
                    else:
                        crops.append({'bb': object['bb'], 'label': object['label'])
                        labels[object['label']].append(index) 
                        index +=1
                else:
                    crops.append({'bb':object['bb'], 'label': object['label'])
                    labels[object['label']] = [index]
                    index +=1
        return (crops, labels)

    def _match_crops(crops1, crops2, label):
        temp2 = list.copy(crops2)
        new_crops = []
        for crop1 in crops1:
            index = -1
            for crop2 in temp2:
                intersect = float(crop1['bb'].intersect_area(crop2['bb']))
                union = float(crop1['bb'].union_area(crop2[i]['bb']))
                iou = intersect/union
                if iou > IOU_THRESHOLD and index == -1:
                    bb = crop1['bb'].union_box(crop2['bb']
                    new_crops.append({'bb': bb, 'label': label})
                    match = True
            if index == -1:
                return None
            del temp2[index]
        return new_crops

    def join(map1, map2):
        """
        Join two batches together if they have the same objects and similar crops.
        Returns None if the two batchs shouldn't be joined
        """
        crop1, labels1 = map1
        crop2, labels2 = map2
        # If the two batches have different number of crops or labels
        # we don't join the crops
        if len(crop1) != len(crop2):
            return None
        if len(labels1) != len(labels2):
            return None

        joined_crops = []
        joined_labels = {}
        indices = 0
        for label in labels1:
            # if the batches have different labels or different number of crops
            # per label, we don't join the crops
            if label not in label2:
                return None
            if len(labels1[label]) != len(labels2[label]):
                return None
            # match the crops in a label with each other
            new_crops = _match_crops(   [crop1[i] for i in labels1[label]], \
                                        [crop2[i] for i in labels2[label]], \
                                        label)
            if new_crops == None:
                return None
            joined_crops.extend(new_crops)
            joined_labels[label] = range(indices, indices + len(new_crops))
            indices = indices + len(new_crops)
        return (joined_crops, joined_labels)