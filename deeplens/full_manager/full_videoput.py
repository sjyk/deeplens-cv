"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered_videoio.py uses opencv (cv2) to read and write files to disk. It contains
primitives to encode and decode archived and regular video formats for a tiered
storage system.
"""

from deeplens.dataflow.map import Resize
#from deeplens.simple_manager.file import *
from deeplens.utils.utils import *
from deeplens.streams import *
from deeplens.pipeline import *
from deeplens.full_manager.full_header_helper import *
from deeplens.utils.constants import *

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


# need to cache to header

class PutOp(Operator):
    def __init__(self, target, args, basedir = DEFAULT_TEMP):
        self.target = target
        self.encoding = args['encoding']
        self.batch_size = args['batch_size']
        self.scale = args['background_scale']
        self.dir = basedir
        self.frame_rate = args['frame_rate']
        self.scoor = None
        self.coor = None

    def __iter__(self):
        self.pipeline = iter(self.pipeline)
        self.meta = CacheFullMetaStream('full_meta')
        vid = self.pipeline.streams['video']
        self.scoor = (int(vid.width * self.scale), int(vid.height*self.scale))
        self.fcoor =  (vid.width, vid.height)  
        return self
    
    def _generate_writers(self, crops):
        r_name = self.target + get_rnd_strng(64)
        self.file_names = []
        self.writers = []
        for i in range(len(crops) + 1):
            seg_name = os.path.join(self.dir, r_name)
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
        index = self.meta.get()
        if index is None:
            raise StopIteration()
        crops, do_join = next(self.pipeline.streams['crops']).get()
        
        if not do_join:
            self._generate_writers(crops)
        self.meta.update_all(index, self.target, crops, self.file_names, self.fcoor, self.scoor, do_join)
        
        for i in range(self.batch_size):
            try:
                frame = next(self.pipeline.streams['video'])
            except StopIteration:
                self.meta.update(None)
                return {'full_meta': self.meta}         
            
            data = frame.get()
            #data_scaled = cv2.resize(frame, self.scoor) # need to check that this copies data
            CVVideoStream.append(data, self.writers[0])
            for j, cr in enumerate(crops):
                fr = crop_box(data, cr['bb'])
                CVVideoStream.append(fr, self.writers[j + 1])
            self.meta.update(index + i + 1)
        return {'full_meta': self.meta}


class HeaderOp(Operator):
    def __init__(self, conn):
        self.conn = conn
        self.ids = None

    def __iter__(self):
        self.index = None
        self.params = CacheStream('header_data')
        return self

    def __next__(self):
        fr = next(self.pipeline)['full_meta']
        #print(fr.crops)
        if fr.do_join:
            params = (self.conn, [fr.crops], fr.name, fr.first_frame, fr.data, self.ids)
            update_headers_batch(*params)
        else:
            params = (self.conn, [fr.crops], fr.name, fr.video_refs, fr.fcoor, fr.scoor, fr.first_frame, fr.data)
            #print(params)
            self.ids = new_headers_batch(*params)
        self.params.update(params)
        return self.params        

#TODO: assumes batch labels from mapOP right now -> fix if needed later
class ConvertMap(Operator):
    def __init__(self, tagger, batch_size):
        self.tagger = tagger
        self.batch_size = batch_size

    def __iter__(self):
        self.labels = CacheStream('labels')
        return self

    def __next__(self):
		# we assume it iterates the entire batch size and save the results
        try:
            tag = self.tagger(self.pipeline.streams['video'], self.batch_size)
        except StopIteration:
            raise StopIteration("Iterator is closed")
        if tag:
            tags = [tag]
        else:
            tags = []
        self.labels.update(tags)

        return {'labels': self.labels}

class ConvertSplit(Operator):
    def __init__(self, splitter):
        self.splitter = splitter

    def __iter__(self):
        self.crops = CacheStream('crops')
        self.initialized = False
        return self

    def __next__(self):
        labels = next(self.pipeline)['labels'].get()
        if not self.initialized:
            crops, self.batch_prev, _ = self.splitter.initialize([labels])
            do_join = False
        else:
            batch = self.splitter.map(labels)
            crops, self.batch_prev, do_join = self.splitter.join(self.batch_prev, batch)
        self.crops.update((crops, do_join))
        return self.crops

def write_video_single(conn, video_file, target, base_dir, splitter, tagger, aux_streams, args, background_scale=1):
    if not os.path.isfile(video_file):
        print("missing file", video_file)
        return None
    if type(conn) == str:
        conn = sqlite3.Connection(conn)
    batch_size = args['batch_size']
    v = CVVideoStream(video_file, target, args['limit'])
    manager_crop = PipelineManager(v)
    if aux_streams != None:
        manager_crop.add_streams(aux_streams)

    manager_crop.add_operators([ConvertMap(tagger, batch_size), ConvertSplit(splitter)])
    crops = manager_crop.build()
    v_main =  CVVideoStream(video_file, args['limit'])
    manager = PipelineManager(v_main)
    manager.add_stream(crops, 'crops')
    manager.add_operator(PutOp(target, args, base_dir))
    manager.add_operator(HeaderOp(conn))
    results = manager.run()
    return results


        





