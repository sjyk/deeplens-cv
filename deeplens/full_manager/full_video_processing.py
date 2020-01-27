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
        data: bounding boxes per frame
        """
        crops = []
        labels = {}
        index = 0
        for objects in data:
            for object in objects:
                if object['label'] in labels:
                    for i in labels[object['label']]:
                        iou = crops.i
                        if 
                        else:
                            temp.append(object)
                            labels[object['label']].append(index) 
                            index +=1
                else:
                    temp.append(object)
                    labels[object['label']] = [index]
                    index +=1