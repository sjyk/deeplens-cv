"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered_videoio.py uses opencv (cv2) to read and write files to disk. It contains
primitives to encode and decode archived and regular video formats for a tiered
storage system.
"""

from deeplens.full_manager.tiered_file import *
from deeplens.constants import *
from deeplens.struct import *
from deeplens.header import *
from deeplens.utils.clip import *
from deeplens.simple_manager.file import *
from deeplens.utils.frame_xform import *

import cv2
import os
from os import path
import time
import shutil
from pathlib import Path
from datetime import datetime

# TODO: WE NEED TO COMPLETELY REWRITE THIS FILE


def write_video_auto(vstream, \
                        output, \
                        encoding, \
                        header_info,
                        output_extern = None, \
                        scratch = DEFAULT_TEMP, \
                        frame_rate=DEFAULT_FRAME_RATE, \
                        header_cmp=RAW):
    """write_video_clips takes a stream of video and writes
    it to disk. It includes the specified header 
    information as a part of the video file. The difference is that 
    it writes a video to disk/external storage from a stream in clips of a specified 
    size

    Args:
        vstream - a videostream or videotransform
        output - output file
        header_info - header information
        clip_size - how many frames in each clip
        scratch - temporary space to use
        frame_rate - the frame_rate of the video
        header_cmp - compression if any on the header
        output_extern - if the file goes to external 
        storage, specify directory
    """
    