"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

frame_xform.py defines transformations per frame
"""
import copy
from deeplens.struct import Box
import cv2

def mask(frame, mask):
    """Masks the content of a frame of the video
    """
    frame = copy.deepcopy(frame)
    frame = frame[mask]
    return frame

def crop_box(frame, box):
    """ Crops a frame of the video to a box 
    given by the input arguments
    """
    frame = copy.deepcopy(frame)
    height, width, channels = frame.shape

    frame = frame[ max(box.y0,0) : min(box.y1,height), \
                   max(box.x0,0) : min(box.x1,width), :]

    #print(frame.shape, box, height, width, min(box.y1,height))
    return frame


def reverse_crop(frame, crops):
    """Masks the content of a frame of the video
    to remove all crops
    """
    frame = copy.deepcopy(frame)
    for crop in crops:
        box = crop['bb']
        frame[box.y0:box.y1, box.x0:box.x1] = 0
    return frame