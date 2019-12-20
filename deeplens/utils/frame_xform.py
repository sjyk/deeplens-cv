"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

frame_xform.py defines transformations per frame
"""
import copy
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
    (x0, y0, x1, y1) = box
    frame = copy.deepcopy(frame)
    frame = frame[y0:y1, x0:x1]
    return frame


def reverse_crop(frame, crops):
    """Masks the content of a frame of the video
    to remove all crops
    """
    frame = copy.deepcopy(frame)
    for crop in crops:
        (x0, y0, x1, y1) = crop
        frame[y0:y1, x0:x1] = 0
    return frame