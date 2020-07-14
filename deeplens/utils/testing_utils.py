"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

utils.py defines some utilities that can be used for debugging and testing our system
"""


def printCrops(crops):
    print('~~~PRINTING CROPS~~~')
    for crop in crops:
        print("Crop: {}".format(crop['label']))
        print("bb: {}".format(crop['bb'].serialize()))

def testGetClipIds(manager, query, order = True):
    clip_ids = manager.get(query)
    

def testGetVideoStreams(manager, query, order = True):
    pass

def testGetPipeline(manager, query, order = True, pipeline = None):
    pass