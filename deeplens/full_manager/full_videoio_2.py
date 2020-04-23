"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered_videoio.py uses opencv (cv2) to read and write files to disk. It contains
primitives to encode and decode archived and regular video formats for a tiered
storage system.
"""

from deeplens.header import *
from deeplens.dataflow.map import Resize
from deeplens.simple_manager.file import *
from deeplens.utils.frame_xform import *
from deeplens.extern.ffmpeg import *
from deeplens.media.youtube_tagger import *
from deeplens.streams import all

import sqlite3
import random

import cv2
import os
from os import path
import time
import shutil
import logging
import json
import itertools
#import threading
#import queue
#from multiprocessing import Pool
#import glob

def write_video_single(conn, \
                        video_file, \
                        target, \
                        dir, \
                        splitter, \
                        map, \
                        start_time=0, \
                        stream=False, #not currently supported
                        args={}, 
                        log=False, # not currently supported
                        background_scale=1): # not currently supported
    
    if not os.path.isfile(video_file):
        print("missing file", video_file)
        return None
    if type(conn) == str:
        conn = sqlite3.Connection(conn)
    batch_size = args['batch_size']
    v = CVVideoStream(video_file, args['limit'])
    v = v[map]
    start_time = start_time
    v_main =  CVVideoStream(video_file, args['limit'])

    v 
    
class SplitterStream(DataStream):

class putOp(Operator):
    def __init__(conn, splitter):
        pass



