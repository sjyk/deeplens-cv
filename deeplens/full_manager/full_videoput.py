"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

full_video_put.py uses opencv (cv2) to read and write files to disk. It contains
primitives to encode and decode archived and regular video formats for a tiered
storage system.
"""
import os
from deeplens.utils.utils import *
from deeplens.streams import *
from deeplens.pipeline import *
from deeplens.full_manager.full_header_helper import *
from deeplens.utils.constants import *
from deeplens.cache_streams import *

import sqlite3
import random


class PutOp(Operator):
    def __init__(self, name, target, args, basedir = DEFAULT_TEMP, input_names = ['video', 'crops'], output_names = ['meta_data']):
        super.__init__(name, input_names)
        self.target = target
        self.encoding = args['encoding']
        self.batch_size = args['batch_size']
        self.scale = args['background_scale']
        self.dir = basedir
        self.frame_rate = args['frame_rate']

        
        self.results[output_names[0]] = CacheStream(output_names[0], self)
        self.meta = self.results[output_names[0]] 

    def __iter__(self):
        super.__iter__()
        self.vid = self.streams[self.input_names[0]]
        self.crops = self.streams[self.input_names[1]]

        self.scoor = (int(vid.width * self.scale), int(vid.height*self.scale))
        self.fcoor =  (vid.width, vid.height)
        self.stop = False
        self.vid_index = 0
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
        super.__iter__()                

        if self.stop:
            raise StopIteration()
        
        crops, do_join = crops.next(self.name)
                
        if not do_join:
            self._generate_writers(crops)
        
        meta = (crops, self.target, self.curr_index, 0, self.file_names,  self.fcoor, self.scoor, do_join)
        
        for i in range(self.batch_size):
            try:
                frame = self.vid.next(self.name)
            except StopIteration:
                self.stop = True
                self.meta.insert(meta)
                return self.index        
            
            #data_scaled = cv2.resize(frame, self.scoor) # need to check that this copies data

            for j, cr in enumerate(crops):
                fr = crop_box(frame, cr['bb'])
                CVVideoStream.append(fr, self.writers[j + 1])
            fdata = reverse_crop(frame, crops)
            CVVideoStream.append(fdata, self.writers[0])
            
            self.curr_index += 1
        meta[3] = self.curr_index
        self.meta.insert(meta)
        return self.index

class HeaderOp(Operator):
    def __init__(self, name, conn, labels = False, input_names = ['meta_data'], output_names = ['ids']):
        super.__init__(name, input_names)
        self.conn = conn
        self.ids = None
        self.labels = labels
        self.results[output_names[0]] = CacheStream(output_names[0])

    def __next__(self):
        super.__next__()
        fr = self.streams[self.input_names[0]].next(self.name)
        #print(fr.crops)
        # meta = (crops, self.target, index, index, self.file_names,  self.fcoor, self.scoor, do_join)
        if fr[7]:
            params = (self.conn, fr[0], fr[1], fr[2], fr[3], self.ids)
            update_headers_batch(*params)
        else:
            params = (self.conn, [fr[0]], fr[1], fr[2], fr[3], fr[4], fr[5], fr[6])
            self.ids = new_headers_batch(*params)
            self.results[self.output_names[0]].insert(self.ids)

        return self.index


class LabelsOp(Operator):
    def __init__(self, name, conn, target, input_names=['ids', 'labels']):
        super.__init__(name, input_names)
        self.conn = conn
        self.vid_name = target
        self.frames = {}
        self.results = None
        self.ids = None

    def __next__(self):
        super.__next__()
        ids = self.streams[self.input_names[0]].next(self.name)
        labels = self.streams[self.input_names[1]].next(self.name)

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
                    
        return self.index


class ConvertMap(Operator):
    def __init__(self, name, tagger, batch_size, multi = True, input_names = ['video'], output_names = ['map_labels']):
        super().__init__(name, input_names)
        self.tagger = tagger
        self.batch_size = batch_size
        self.multi = multi # if the output has multiple frames -> don't need to wrap
        self.results = {}
        self.output_name = output_names[0]
        self.results[output_names] = CacheStream(output_names, self)

    def __next__(self):
        super().__next__()
		# we assume it iterates the entire batch size and save the results
        try:
            tag = self.tagger(self.streams, self.name, self.batch_size) # TODO: need to update miris experiments !!
        except StopIteration:
            raise StopIteration("Iterator is closed")
        if tag and self.multi:
            tags = tag
        elif tag:
            tags = [tag]
        else:
            tags = []
        self.results[self.output_name].insert(tags)

        return self.index

class ConvertSplit(Operator):
    def __init__(self, name, splitter, input_names = ['map_labels'], output_names = ['crops', 'labels']):
        super().__init__(name, input_names)
        self.splitter = splitter
        self.results = {}
        self.output_names = output_names
        self.results[output_names[0]] = CacheStream(output_names[0], self)
        self.results[output_names[1]] = CacheStream(output_names[1], self)

    def __iter__(self):
        super.__iter__()
        self.ilabels = self.streams[self.input_names[0]]
        self.initialized = False
        self.ilabels.add_iter(self.name)
        return self

    def __next__(self):
        super.__next__()
        labels = self.ilabels.next(self.name)

        if not self.initialized:
            crops, self.batch_prev, _ = self.splitter.initialize(labels)
            self.initialized = True
            do_join = False
        else:
            batch = self.splitter.map(labels)
            crops, self.batch_prev, do_join = self.splitter.join(self.batch_prev, batch)
        
        cropn = self.output_names[0]
        lbln = self.output_names[1]
        self.results[cropn].insert((crops[0], do_join))
        self.results[lbln].insert(crops[1])
        
        return self.index

def write_video_single(conn, video_file, target, base_dir, splitter, tagger, args, labels = None, only_labels = False, background_scale=1):
    if not os.path.isfile(video_file):
        print("missing file", video_file)
        return None

    if type(conn) == str:
        conn = sqlite3.Connection(conn)
    batch_size = args['batch_size']

    manager = GraphManager()
    manager.add_stream(CVVideoStream(video_file, 'video', args['limit']), 'video')

    # link appropriate stream to map
    if labels != None:
        manager.add_streams(labels)
        lnames = set(labels.keys())
        if not only_labels:
            lnames.add('video')
        manager.add_operator(ConvertMap('map', tagger, batch_size, input_names=lnames))
    else:
        manager.add_operator(ConvertMap('map', tagger, batch_size, input_names=['video']))
    
    
    manager.add_operator(ConvertSplit('crops'))
    
    manager.add_operator(PutOp('put', target, args, base_dir))
    
    manager.add_operator(HeaderOp('headers', conn, target))
    manager.add_operator(LabelsOp('labels', conn, target, results = 'ids'), dstreams = {'headers', 'labels'})
    
    #manager.draw()
    results = manager.run(results = 'ids')
    
    return results