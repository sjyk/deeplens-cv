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


# need to cache to header

class PutOp(Operator):
    def __init__(self, vid_name, encoding, frame_rate, batch_size, condense = True, scale = 1, dir = DEFAULT_TEMP):
        self.encoding = encoding
        self.vid_name = name
        self.batch_size = batch_size
        self.scale = scale
        self.dir = dir
        self.frame_rate = frame_rate
        self.scoor = None

    def __iter__(self):
        self.pipeline = iter(self.pipeline)
        self.meta = CacheFullMetaStream('full_meta')
        return self
    
    def _generate_writers(self, crops):
        r_name = vid_name + get_rnd_strng(64)
        self.file_names = []
        self.writers = []
        for i in range(len(crops) + 1):
            seg_name = os.path.join(dir, r_name)
            file_name = add_ext(seg_name, AVI, i)
            self.file_names.append(file_name)
            if i == 0:
                out = CVVideoStream.init_mat(file_name, self.encoding, self.scoor[0], self.scoor[1], self.frame_rate)
            else:
                width = abs(crops[i - 1]['bb'].x1 - crops[i - 1]['bb'].x0)
                height = abs(crops[i - 1]['bb'].y1 - crops[i - 1]['bb'].y0)
                out = CVVideoStream.init_mat(file_name, self.encoding, width, height, self.frame_rate)

            self.writers.append(out)
            
    def __next__(self):
        frame = next(self.pipeline.streams['video'])
        index = self.meta.get()
        if index == 0:
            self.scoor = (int(frame.width* self.scale), int(fram.hieght*self.scale))

        if index % self.batch_size == 0:
            crops, do_join = next(self.pipeline.streams['crops']).get()
            if not do_join:
                self._generate_writers(crops)
        
        data = frame.get()
        data_scaled = cv2.resize(frame, self.scoor) # need to check that this copies data
        CVVideoStream.append(data_scaled, self.writers[0])

        for i, cr in enumerate(self.crops):
            fr = crop_box(data, cr['bb'])
            self.writers[i].append(fr)
        
        if index == 0:
            self.meta.update_all(self.vid_name, crops, self.file_names, (frame.width, frame.height), scoor, do_join)
        else:
            self.meta.update(index + 1)
        frame['full_meta'] = self.meta
        return frame


class HeaderOp(Operator):
    def __init__(self, conn):
        self.conn = conn

    def __iter__(self):
        self.curr_data = N*[None]
        return self

    def __next__(self):
        try:
            frame = next(self.pipeline['full_meta'])
        except:
            pass # TODO
        index = frame.get
        

        

def write_video_single(conn, video_file, target, dir, splitter, map, args, background_scale=1):
    if not os.path.isfile(video_file):
        print("missing file", video_file)
        return None
    if type(conn) == str:
        conn = sqlite3.Connection(conn)
    batch_size = args['batch_size']
    v = CVVideoStream(video_file, args['limit'])
    crops = Pipeline({'video':v})[ConvertMap(map)][ConvertSplit(split, batch_size)]

    v_main =  CVVideoStream(video_file, args['limit'])
    manager = PipelineManager(v_main)
    maanger.add_stream(crops, 'crops')
    manager.add_operator(PutOp)
    manager.add_operator(HeaderOp)
    results = manager.run()
    return results


        





