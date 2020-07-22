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
    def __init__(self, target, args, save_labels, basedir = DEFAULT_TEMP):
        self.target = target
        self.encoding = args['encoding']
        self.batch_size = args['batch_size']
        self.scale = args['background_scale']
        self.dir = basedir
        self.frame_rate = args['frame_rate']
        self.scoor = None
        self.coor = None
        self.save_labels = save_labels

    def __iter__(self):
        self.meta = CacheFullMetaStream('full_meta', 'cache')
        vid = self.streams['video']
        self.streams['full_meta'] = self.meta
        self.scoor = (int(vid.width * self.scale), int(vid.height*self.scale))
        self.fcoor =  (vid.width, vid.height)
        self.stop = False
        return self
    
    def _generate_writers(self, crops):
        r_name = self.target + get_rnd_strng(64)
        self.file_names = []
        self.writers = []
        for i in range(len(crops) + 1):
            seg_name = os.path.join(self.dir, r_name)
            file_name = add_ext(seg_name, MKV, i)
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
        if self.stop:
            raise StopIteration()
        if len(self.pipeline.pipelines) == 0:
            crops, do_join = next(self.streams['crops']).get()
        else:
            labels_stream = next(self.pipeline.pipelines['crops'])
            crops, do_join = labels_stream['crops'].get()
        if self.save_labels:
            self.streams['labels'] = labels_stream['labels']
        
        if not do_join:
            self._generate_writers(crops)
        self.meta.update_all(index, self.target, crops, self.file_names, self.fcoor, self.scoor, do_join)
        
        for i in range(self.batch_size):
            try:
                frame = next(self.streams['video'])
            except StopIteration:
                self.stop = True
                return {'full_meta': self.meta}         
            
            data = frame.get()
            #data_scaled = cv2.resize(frame, self.scoor) # need to check that this copies data
            #print(crops)
            for j, cr in enumerate(crops):
                fr = crop_box(data, cr['bb'])
                CVVideoStream.append(fr, self.writers[j + 1])
            fdata = reverse_crop(data, crops)
            CVVideoStream.append(fdata, self.writers[0])
            self.meta.update(index + i + 1)
        return self.streams

class HeaderOp(Operator):
    def __init__(self, conn, labels = False):
        self.conn = conn
        self.ids = None
        self.labels = labels

    def __iter__(self):
        self.index = None
        self.cache_ids = CacheStream('header_ids')
        self.streams['header_ids'] = self.cache_ids
        return self

    def __next__(self):
        fr = next(self.pipeline)['full_meta']
        #print(fr.crops)
        if fr.do_join:
            params = (self.conn, fr.crops, fr.name, fr.first_frame, fr.data, self.ids)
            update_headers_batch(*params)
        else:
            params = (self.conn, [fr.crops], fr.name, fr.video_refs, fr.fcoor, fr.scoor, fr.first_frame, fr.data)
            self.ids = new_headers_batch(*params)
            self.cache_ids.update(self.ids)
        return self.streams    

class LabelsOp(Operator):
    def __init__(self, conn, target):
        self.conn = conn
        self.vid_name = target
        self.frames = {}

    def __iter__(self):
        self.ids = None
        return self

    def __next__(self):
        streams = next(self.pipeline)
        ids = streams['header_ids'].get()
        labels = streams['labels'].get()
        for label in labels:
            if label not in self.frames:
                self.frames[label] = {}
            for crop in labels[label]:
                data = labels[label][crop]
                if self.ids is not None and len(self.ids) == len(ids) and self.ids[crop] == ids[crop]:
                    try:
                        self.frames[label][crop] += data.size()
                        update_label_header(self.conn, ids[crop], self.vid_name, label, data.type)
                    except:
                        insert_label_header(self.conn, label, data.serialize(), ids[crop], self.vid_name, data.type)
                else:
                    frames = data.size()
                    insert_label_header(self.conn, label, data.serialize(), ids[crop], self.vid_name, data.type, frames)
                    self.frames[label][crop] = frames
                    
        self.ids = ids
        
        return self.streams


#TODO: assumes batch labels from mapOP right now -> fix if needed later
class ConvertMap(Operator):
    def __init__(self, tagger, batch_size, only_labels = False, multi = True):
        self.tagger = tagger
        self.batch_size = batch_size
        self.multi = multi
        self.only_labels = False

    def __iter__(self):
        self.labels = CacheStream('labels', 'cache_labels')
        self.streams['labels'] = self.labels
        if self.only_labels:
            del self.streams['video']
        return self

    def __next__(self):
		# we assume it iterates the entire batch size and save the results
        try:
            tag = self.tagger(self.pipeline.streams, self.batch_size)
        except StopIteration:
            raise StopIteration("Iterator is closed")
        if tag and self.multi:
            tags = tag
        elif tag:
            tags = [tag]
        else:
            tags = []
        self.labels.update(tags)
        return self.streams

class ConvertSplit(Operator):
    def __init__(self, splitter, save_labels):
        self.splitter = splitter
        self.save_labels = save_labels

    def __iter__(self):
        self.crops = CacheStream('crops', 'bound_boxes')
        self.streams['crops'] = self.crops
        self.initialized = False
        return self

    def __next__(self):
        labels = next(self.pipeline)['labels'].get()
        if not self.initialized:
            crops, self.batch_prev, _ = self.splitter.initialize(labels)
            self.initialized = True
            do_join = False
        else:
            batch = self.splitter.map(labels)
            crops, self.batch_prev, do_join = self.splitter.join(self.batch_prev, batch)
        if self.save_labels:
            self.crops.update((crops[0], do_join))
            self.streams['labels'].update(crops[1])
        else:
            self.crops.update((crops, do_join))
        return self.streams

def write_video_single(conn, video_file, target, base_dir, splitter, tagger, args, labels = None, only_labels = False, save_labels = True, background_scale=1):
    if not os.path.isfile(video_file):
        print("missing file", video_file)
        return None
    if type(conn) == str:
        conn = sqlite3.Connection(conn)
    batch_size = args['batch_size']
    v = CVVideoStream(video_file, 'label_video', args['limit'])
    manager_crop = PipelineManager(v)
    if labels != None:
        manager_crop.add_streams(labels)

    manager_crop.add_operators([ConvertMap(tagger, batch_size, only_labels), ConvertSplit(splitter, save_labels)])
    crops = manager_crop.build()
    v_main =  CVVideoStream(video_file, target, args['limit'])
    manager = PipelineManager(v_main)
    manager.add_pipeline(crops, 'crops')
    manager.add_operator(PutOp(target, args, save_labels, base_dir))
    manager.add_operator(HeaderOp(conn, target))
    if save_labels and labels is not None:
        manager.add_operator(LabelsOp(conn, target))
    results = manager.run('header_ids')
    return results

def write_video_fixed(conn, video_file, target, base_dir, crops, args, labels = None, save_labels = False, background_scale=1):
    if not os.path.isfile(video_file):
        print("missing file", video_file)
        return None
    if type(conn) == str:
        conn = sqlite3.Connection(conn)
    batch_size = args['batch_size']
    v_main =  CVVideoStream(video_file, target, args['limit'])
    manager = PipelineManager(v_main)
    manager.add_operator(crops, 'crops')
    manager.add_operator(PutOp(target, args, save_labels, base_dir))
    manager.add_operator(HeaderOp(conn, target))
    if save_labels and labels is not None:
        manager.add_operator(LabelsOp(conn, target))
    results = manager.run('header_ids')
    #print(results)
    return results





