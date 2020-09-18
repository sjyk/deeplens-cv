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
import json

import sqlite3
import random


class PutCropOp(Operator):
    def __init__(self, name, vid_name, args, start_time=0, basedir = DEFAULT_TEMP, input_names = ['video', 'crops'], output_names = ['meta_data']):
        super().__init__(name, input_names, output_names)
        self.vid_name = vid_name
        self.encoding = args['encoding']
        self.batch_size = args['batch_size']
        self.scale = args['background_scale']
        self.dir = basedir
        self.frame_rate = args['frame_rate']
        self.time = start_time
        self.results[output_names[0]] = CacheStream(output_names[0], self)
        self.meta = self.results[output_names[0]] 

    def __iter__(self):
        super().__iter__()
        self.vid = self.streams[self.input_names[0]]
        self.crops = self.streams[self.input_names[1]]

        self.scoor = (int(self.vid.width * self.scale), int(self.vid.height*self.scale))
        self.fcoor =  (self.vid.width, self.vid.height)
        self.curr_index = 0
        return self
    
    def _generate_writers(self, crops):
        r_name = self.vid_name + get_rnd_strng(64)
        self.file_names = []
        self.writers = []
        
        for i in range(len(crops) + 1):
            seg_name = os.path.join(self.dir, r_name)
            file_name = add_ext(seg_name, MKV, i)
            self.file_names.append(file_name)
            if i == 0:
                out = CVVideoStream.init_mat(file_name, self.encoding, self.scoor[0], self.scoor[1], self.frame_rate)
            else:
                width = abs(crops[i - 1].x1 - crops[i - 1].x0)
                height = abs(crops[i - 1].y1 - crops[i - 1].y0)
                out = CVVideoStream.init_mat(file_name, self.encoding, width, height, self.frame_rate)

            self.writers.append(out)
            
    def __next__(self):
        super().__next__()
        crops, do_join = self.crops.next(self.name)
        meta = []
        for i in range(self.batch_size):
            try:
                frame = self.vid.next(self.name)
            except StopIteration:
                if i > 0:
                    meta[3] = self.curr_index
                    meta[5] = self.time
                    self.meta.insert(meta)
                raise StopIteration()
            
            if i == 0:
                if not do_join:
                    self._generate_writers(crops)
                    self.ids = [random.getrandbits(63) for i in range(len(crops) + 1)] 
                meta = [crops, self.vid_name, self.curr_index, 0, self.time, 0, self.file_names,  self.fcoor, self.scoor, self.ids, do_join]

            #data_scaled = cv2.resize(frame, self.scoor) # need to check that this copies data

            for j, cr in enumerate(crops):
                fr = crop_box(frame, cr)
                CVVideoStream.append(fr, self.writers[j + 1])
            
            fdata = reverse_crop(frame, crops)
            data_scaled = cv2.resize(fdata, self.scoor) 
            CVVideoStream.append(fdata, self.writers[0])
            
            self.curr_index += 1
            self.time += 1.0/self.batch_size
        meta[3] = self.curr_index
        meta[5] = self.time
        self.meta.insert(meta)
        return self.index

class PutOp(Operator):
    def __init__(self, name, vid_name, args, start_time=0, basedir = DEFAULT_TEMP, input_names = ['video'], output_names = ['meta_data']):
        super().__init__(name, input_names, output_names)
        self.vid_name = vid_name
        self.encoding = args['encoding']
        self.batch_size = args['batch_size']
        self.scale = args['background_scale']
        self.dir = basedir
        self.frame_rate = args['frame_rate']
        self.results[output_names[0]] = CacheStream(output_names[0], self)
        self.meta = self.results[output_names[0]]
        self.time = start_time

    def __iter__(self):
        super().__iter__()
        self.vid = self.streams[self.input_names[0]]
        self.scoor = (int(self.vid.width * self.scale), int(self.vid.height*self.scale))
        self.fcoor =  (self.vid.width, self.vid.height)
        self.curr_index = 0        
        return self
    
    def __next__(self):
        super().__iter__()                
        
        ids = [random.getrandbits(63)]        
        r_name = self.vid_name + get_rnd_strng(64)        
        seg_name = os.path.join(self.dir, r_name)
        self.file_name = add_ext(seg_name, MKV)
        meta = [[], self.vid_name, self.curr_index, 0, self.time, 0, [self.file_name],  self.fcoor, self.scoor, ids, False]
        
        for i in range(self.batch_size):
            try:
                frame = self.vid.next(self.name)
            except StopIteration:
                if i > 0:
                    meta[3] = self.curr_index
                    meta[5] = self.time
                    self.meta.insert(meta)
                raise StopIteration()
            
            if i == 0:
                self.writer = CVVideoStream.init_mat(self.file_name, self.encoding, self.scoor[0], self.scoor[1], self.frame_rate)
            
            data_scaled = cv2.resize(frame, self.scoor) # need to check that this copies data
            CVVideoStream.append(data_scaled, self.writer)
            
            self.curr_index += 1
            self.time += 1.0/self.batch_size
        meta[3] = self.curr_index
        meta[5] = self.time
        self.meta.insert(meta)
        
        return self.index

class HeaderOp(Operator):
    def __init__(self, name, conn, labels = False, input_names = ['meta_data'], output_names = ['ids']):
        super().__init__(name, input_names, output_names)
        self.conn = conn
        self.labels = labels
        self.results[output_names[0]] = CacheStream(output_names[0], self)

    def __next__(self):
        super().__next__()
        fr = self.streams[self.input_names[0]].next(self.name)
        if fr[10]:
            params = (self.conn, fr[0], fr[1], fr[2], fr[3], fr[4], fr[5], fr[9])
            update_headers_batch(*params)
            self.results[self.output_names[0]].insert(self.ids)
        else:
            params = (self.conn, [fr[0]], fr[1], fr[2], fr[3], fr[4], fr[5], fr[6], fr[7], fr[8], fr[9])
            self.ids = new_headers_batch(*params)
            self.results[self.output_names[0]].insert(self.ids)
        return self.index


class LabelMapOp(Operator):
    def __init__(self, name, conn, vid_name, dtype = 'JSONListStream', label_db = 'label' , input_names=['map_labels', 'meta_data']):
        super().__init__(name, input_names, [])
        self.conn = conn
        self.vid_name = vid_name
        self.frames = {}
        self.ids = None
        self.label_db = label_db
        self.dtype = dtype


    def __next__(self):
        super().__next__()
        meta = self.streams[self.input_names[1]].next(self.name)
        labels = self.streams[self.input_names[0]].next(self.name)
        ids = meta[-2]
        crops = meta[0]
        for frame in labels:
            for label in frame:
                indices = []
                if 'bb' in label:
                    for i, crop in enumerate(crops):
                        if crop.contains(label['bb']):
                            indices.append(i + 1)
                if len(indices) == 0:
                    indices.append(0)
                for i in indices:
                    if 'value' in label:
                        value = label['value']
                    else:
                        value = None
                    if 'bb' in label:
                        bb = json.dumps((label['bb'].serialize()))
                    else:
                        bb = None
                    if 'frame' in label:
                        frame = label['frame']
                    else:
                        frame = None
                    insert_label_header(self.conn, label['label'], ids[i], self.vid_name, data_type = self.dtype, value = value, bbox = bb, frame = frame, db_name = self.label_db)
        return self.index

class LabelOp(Operator):
    def __init__(self, name, conn, vid_name, batch_size, label_db = 'label', input_names=['labels', 'meta']):
        super().__init__(name, input_names, [])
        self.conn = conn
        self.vid_name = vid_name
        self.ids = None
        self.label_db = label_db
        self.has_time = False    
        self.batch_size = batch_size
        
    
    def __iter__(self):
        super().__iter__()
        self.labels = self.streams[self.input_names[0]]
        self.ttype = self.labels.ttype
        self.iter_meta()
        return self

    def iter_meta(self):
        meta = self.streams[self.input_names[1]].next(self.name)
        self.crops = meta[0]
        self.ids = meta[-2]
        self.frames = (meta[2], meta[3])
        self.times = (meta[4], meta[5])


    def __next__(self):
        super().__next__()
        label = self.labels.next(self.name)
        dtype = str(type(self.streams[self.input_names[0]]))
        start = dtype.find("'")
        end = dtype.rfind("'")
        dtype = dtype[start + 1:end]
        indices = []
        if 'frame' in label:
            while True:
                if self.ttype == 'frame':
                    if label['frame'] < self.frames[0]:
                        raise IndexError("DataStream is not in order")
                    elif label['frame'] >= self.frames[1]:
                        self.iter_meta()
                    else:
                        break
                elif self.ttype == 'time':
                    if label['frame'] < self.times[0]:
                        raise IndexError("DataStream is not in order")
                    elif label['frame'] >= self.times[1]:
                        self.iter_meta()
                    else:
                        break
                else:
                    raise ValueError("ttype value needs to be 'frame' or 'time'")
        else:
            if self.index % self.batch_size == 0:
                self.iter_meta()
        
        if 'bb' in label:
            for i, crop in enumerate(self.crops):
                if crop['bb'].contains(label['bb']):
                    indices.append(i + 1)
        if len(indices) == 0:
            indices.append(0)
        for i in indices:
            if 'value' in label:
                value = label['value']
            else:
                value = None
            if 'bb' in label:
                bb = json.dumps((label['bb'].serialize()))
            else:
                bb = None
            if 'frame' in label:
                frame = label['frame']
            else:
                frame = None
            insert_label_header(self.conn, label['label'], self.ids[i], self.vid_name, data_type = dtype, value = value, bbox = bb, frame = frame, db_name = self.label_db)
    
        return self.index

class ConvertMap(Operator):
    def __init__(self, name, tagger, batch_size, multi = True, input_names = ['video'], output_names = ['map_labels']):
        super().__init__(name, input_names, output_names)
        self.tagger = tagger
        self.batch_size = batch_size
        self.multi = multi # if the output has multiple frames -> don't need to wrap
        self.results[output_names[0]] = CacheStream(output_names[0], self)
        

    def __next__(self):
        super().__next__()
		# we assume it iterates the entire batch size and save the results
        tag = self.tagger(self.streams, self.name, self.batch_size, self.batch_size*self.index) # TODO: need to update miris experiments !!
        if tag and self.multi:
            tags = tag
        elif tag:
            tags = [tag]
        else:
            tags = []
        self.results[self.output_names[0]].insert(tags)

        return self.index

class ConvertSplit(Operator):
    def __init__(self, name, splitter, input_names = ['map_labels'], output_names = ['crops']):
        super().__init__(name, input_names, output_names)
        self.splitter = splitter
        self.results[output_names[0]] = CacheStream(output_names[0], self)        

    def __iter__(self):
        super().__iter__()
        self.initialized = False
        self.labels = self.streams[self.input_names[0]]
        return self

    def __next__(self):
        super().__next__()
        labels = self.labels.next(self.name)

        if not self.initialized:
            crops, self.batch_prev, do_join = self.splitter.initialize(labels)
            self.initialized = True
        else:
            batch = self.splitter.map(labels)
            crops, self.batch_prev, do_join = self.splitter.join(self.batch_prev, batch)
        
        self.results[self.output_names[0]].insert((crops, do_join))        
        return self.index

def write_video_single(conn, vstream, name, base_dir, splitter, tagger, args, map_streams, aux_streams, fixed, start_time = 0):
    # get vstream
    if type(vstream) == int:
            stream = CVRealVideoStream(vstream, 'video', args['limit'],offset=args['offset'])
    elif type(vstream) == str:
        if not os.path.isfile(vstream):
            print("missing file", vstream)
            return None
        stream = CVVideoStream(vstream, 'video', args['limit'], offset=args['offset'])
    else:
        stream = stream

    #
    if type(conn) == str:
        conn = sqlite3.Connection(conn)
    batch_size = args['batch_size']

    manager = GraphManager()
    manager.add_stream(stream)

    # link appropriate stream to map
    if map_streams != None:
        for stream in map_streams:
            if isinstance(map_streams[stream], DataStream):
                manager.add_stream(map_streams[stream])
            else:
                s = map_streams[stream]
                dstream = sname_to_class(s[1])(s[0], stream)
                manager.add_stream(dstream)
    
    if aux_streams != None:
        for stream in aux_streams:
            if isinstance(aux_streams[stream], DataStream):
                manager.add_stream(aux_streams[stream])
            else:
                s = aux_streams[stream]
                dstream = sname_to_class(s[1])(s[0], stream)
                manager.add_stream(dstream)

    if map_streams != None and not fixed:
        lnames = list(map_streams.keys())
        lnames.append('video')
        manager.add_operator(ConvertMap('map', tagger, batch_size, input_names=lnames))
        manager.add_operator(ConvertSplit('crops', splitter))
        manager.add_operator(PutCropOp('put', name, args, start_time, base_dir))
    elif not fixed:
        manager.add_operator(ConvertMap('map', tagger, batch_size, input_names=['video']))
        manager.add_operator(ConvertSplit('crops', splitter))
        manager.add_operator(PutCropOp('put', name, args, start_time, base_dir))
    
    elif map_streams != None and fixed:
        if len(map_streams) > 1:
            raise ValueError('map_streams can only take one stream (crops) when it is fixed')
        lnames = ['video', list(map_streams.keys())[0]]
        ret = manager.add_operator(PutCropOp('put', name, args, start_time, base_dir, input_names=lnames))
    else:
        ret = manager.add_operator(PutOp('put', name, args, start_time, base_dir))
        
    manager.add_operator(HeaderOp('headers', conn, name))
    if not fixed:
        manager.add_operator(LabelMapOp('add_map_labels', conn, name))
        op_names = ['headers', 'add_map_labels']
    else:
        op_names = ['headers']

    if aux_streams != None:
        for stream in aux_streams:
            op = 'add_{}'.format(stream)
            ret = manager.add_operator(LabelOp(op, conn, name, batch_size, input_names=[stream,'meta_data']))
            op_names.append(op)
    results = manager.run(op_names)
    
    return results