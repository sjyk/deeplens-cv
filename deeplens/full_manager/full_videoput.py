"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered_videoio.py uses opencv (cv2) to read and write files to disk. It contains
primitives to encode and decode archived and regular video formats for a tiered
storage system.
"""

from deeplens.dataflow.map import Resize
from deeplens.simple_manager.file import *
from deeplens.utils.utils import *
from deeplens.streams import *
from deeplens.pipeline import *
from deeplens.full_manager.full_header_helper import *

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
    def __init__(self, vid_name, encoding, frame_rate, batch_size, condense = True, scale = 1, dir = DEFAULT_TEMP):
        self.encoding = encoding
        self.vid_name = vid_name
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
        
        if index % 0:
            self.meta.update_all(index, self.vid_name, crops, self.file_names, (frame.width, frame.height), scoor, do_join)
        else:
            self.meta.update(index + 1)
        frame['full_meta'] = self.meta
        return frame


class HeaderOp(Operator):
    def __init__(self, conn):
        self.conn = conn
        

    def __iter__(self):
        self.index = None
        self.params = None
        self.stop = False
        return self

    def __next__(self):
        while True:
            if self.stop:
                raise StopIteration()
            try:
                frame = next(self.pipeline['full_meta'])
            except StopIteration():
                new_headers_batch(self.conn, *self.params.get())
                self.stop = True
                return self.params
            index = frame.get()
            if frame.first_frame == index:
                if self.params != None:
                    new_headers_batch(self.conn, *self.params.get())
                old_params = self.params
                params = [[frame.crops], frame.vid_name, frame.video_refs, frame.fd, frame.sd, index, index]
                self.params = CacheStream('header_data')
                self.params.update(params)
                return old_params
            else:
                params = self.params.get()
                params[0].append(frame.crops)
                if frame.new_batch:
                    params[0].append(frame.crops)
                params[6] = index
                self.params.update(params)
            return self.params        

#TODO: assumes batch labels from mapOP right now -> fix if needed later
def ConvertMap(Operator):
    def __init__(self, tagger, batch_size):
        self.tagger = tagger
        self.batch_size = batch_size

    def __iter__(self):
        self.labels = CacheStream('labels')

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
        return self.labels

def ConvertSplit(Operator):
    def __init__(self, splitter):
        self.splitter = splitter

    def __iter__(self):
        self.crops = CacheStream('crops')
        self.initialized = False

    def __next__(self):
        labels = self.pipeline.next()['labels'].get()
        if not self.initialized:
            crops, self.batch_prev, _ = self.splitter.initilaize(labels)
            do_join = False
        else:
            batch = self.splitter.map(labels)
            crops, self.batch_prev, do_join = self.splitter.join(self.batch_prev, batch)
        self.crops.update((crops, do_join))
        return self.crops

def write_video_single(conn, video_file, target, dir, splitter, tagger, aux_streams, args, background_scale=1):
    if not os.path.isfile(video_file):
        print("missing file", video_file)
        return None
    if type(conn) == str:
        conn = sqlite3.Connection(conn)
    batch_size = args['batch_size']
    v = CVVideoStream(video_file, args['limit'])
    manager_crop = PipelineManager(v)
    if aux_streams != None:
        manager_crop.add_streams(aux_streams)
    manager_crop.add_operators([ConvertMap(tagger, batch_size), ConvertSplit(splitter)])
    crops = manager_crop.build()

    v_main =  CVVideoStream(video_file, args['limit'])
    manager = PipelineManager(v_main)
    manager.add_stream(crops, 'crops')
    manager.add_operator(PutOp)
    manager.add_operator(HeaderOp)
    results = manager.run()
    return results


        





