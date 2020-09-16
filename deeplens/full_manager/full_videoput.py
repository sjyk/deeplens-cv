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


class PutCropOp(Operator):
    def __init__(self, name, vid_name, args, start_time=0, basedir = DEFAULT_TEMP, input_names = ['video', 'crops'], output_names = ['meta_data']):
        super.__init__(name, input_names)
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
        super.__iter__()
        self.vid = self.streams[self.input_names[0]]
        self.crops = self.streams[self.input_names[1]]

        self.scoor = (int(vid.width * self.scale), int(vid.height*self.scale))
        self.fcoor =  (vid.width, vid.height)
        self.stop = False
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
            self.ids = [random.getrandbits(63) for i in range(len(crops) + 1)] 

        meta = (crops, self.vid_name, self.curr_index, 0, self.time, 0, self.file_names,  self.fcoor, self.scoor, self.ids, do_join)
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
        super.__init__(name, input_names)
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
        super.__iter__()
        self.vid = self.streams[self.input_names[0]]
        self.scoor = (int(vid.width * self.scale), int(vid.height*self.scale))
        self.fcoor =  (vid.width, vid.height)
        self.stop = False
        return self
    
    def __next__(self):
        super.__iter__()                

        if self.stop:
            raise StopIteration()
        
        ids = [random.getrandbits(63)]        

        meta = ([], self.vid_name, self.curr_index, 0, self.time, 0, [self.file_name],  self.fcoor, self.scoor, ids, False)
        r_name = self.vid_name + get_rnd_strng(64)        
        seg_name = os.path.join(self.dir, r_name)
        self.file_name = add_ext(seg_name, MKV)
        self.writer = CVVideoStream.init_mat(file_name, self.encoding, self.scoor[0], self.scoor[1], self.frame_rate)
        
        for i in range(self.batch_size):
            try:
                frame = self.vid.next(self.name)
            except StopIteration:
                self.stop = True
                self.meta.insert(meta)
                return self.index        
            
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
        super.__init__(name, input_names)
        self.conn = conn
        self.ids = None
        self.labels = labels
        self.results[output_names[0]] = CacheStream(output_names[0])

    def __next__(self):
        super.__next__()
        fr = self.streams[self.input_names[0]].next(self.name)
        #print(fr.crops)
        # meta = (crops, self.target, index, index, start_time, end_time, self.file_names,  self.fcoor, self.scoor, do_join)
        if fr[9]:
            params = (self.conn, fr[0], fr[1], fr[2], fr[3], self.ids)
            update_headers_batch(*params)
            self.results[self.output_names[0]].insert(self.ids)
        else:
            params = (self.conn, [fr[0]], fr[1], fr[2], fr[3], fr[4], fr[5], fr[6], fr[7], fr[8])
            self.ids = new_headers_batch(*params)
            self.results[self.output_names[0]].insert(self.ids)
        return self.index


class LabelMapOp(Operator):
    def __init__(self, name, conn, vid_name, label_db = 'labels', input_names=['labels', 'meta']):
        super.__init__(name, input_names)
        self.conn = conn
        self.vid_name = vid_name
        self.frames = {}
        self.results = None
        self.ids = None
        self.label_db = label_db


    def __next__(self):
        super.__next__()
        labels = self.streams[self.input_names[0]].next(self.name)
        ids = self.streams[self.input_name[1]].next(self.name)[-2]
        crops = self.streams[self.input_name[1]].next(self.name)[0]
        dtype = str(type(labels))
        start = dtype.find("'")
        end = dtype.rfind("'")
        dtype = dtype[start + 1:end]
        for label in labels:
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
                    bb = label['bb']
                else:
                    bb = None
                if 'frame' in label:
                    frame = label['frame']
                else:
                    frame = None
                insert_label_header(self.conn, label['label'], ids[i], self.vid_name, data_type = dtype, value = value, bbox = bb, frame = frame, db_name = self.label_db)
        
        return self.index

class LabelOp(Operator):
    def __init__(self, name, conn, vid_name, batch_size, ttype = 'frame', label_db = 'labels', input_names=['labels', 'meta']):
        super.__init__(name, input_names)
        self.conn = conn
        self.vid_name = vid_name
        self.frames = {}
        self.results = None
        self.ids = None
        self.label_db = label_db
        self.has_time = False
        self.ttype = ttype
        self.batch_size = batch_size
        self.labels = self.streams[self.input_names[0]]
    
    def iter_meta(self):
        meta = self.streams[self.input_name[1]].next(self.name)
        self.crops = meta[0]
        self.ids = meta[-2]
        self.frames = (meta[2], meta[3])
        self.times = (meta[4], meta[5])


    def __next__(self):
        super.__next__()
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
            if self.index % self.batch_size:
                self.iter_meta()
        
        if 'bb' in label:
            for i, crop in enumerate(self.crops):
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
                bb = label['bb']
            else:
                bb = None
            if 'frame' in label:
                frame = label['frame']
            else:
                frame = None
            insert_label_header(self.conn, label['label'], self.ids[i], self.vid_name, data_type = dtype, value = value, bbox = bb, frame = frame, db_name = self.label_db)
    
        return self.index

class ConvertMap(Operator):
    def __init__(self, name, tagger, batch_size, multi = False, input_names = ['video'], output_names = ['map_labels']):
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
            tag = self.tagger(self.streams, self.name, self.batch_size, self.index*self.batch_size) # TODO: need to update miris experiments !!
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
    def __init__(self, name, splitter, input_names = ['map_labels'], output_names = ['crops']):
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

def write_video_single(conn, vstream, name, base_dir, splitter, tagger, args, aux_streams, fixed):
    if type(vstream) == int:
            stream = CVRealVideoStream(vstream, 'video', args['limit'],offset=args['offset'])
    elif type(vstream) == str:
        if not os.path.isfile(vstream):
            print("missing file", vstream)
            return None
        stream = CVVideoStream(vstream, 'video', args['limit'], offset=args['offset'])
    else:
        stream = stream

    if type(conn) == str:
        conn = sqlite3.Connection(conn)
    batch_size = args['batch_size']

    manager = GraphManager()
    manager.add_stream(CVVideoStream(vstream, 'video', args['limit']), 'video')

    # link appropriate stream to map
    if aux_streams != None:
        for stream in aux_streams:
            if isinstance(aux_streams[stream], DataStream):
                manager.add_stream(aux_streams[stream])
            else:
                s = aux_streams[stream]
                dstream = sname_to_class(s[1])(s[0], stream)
                manager.add_stream(dstream)
    #if fixed:

    if aux_streams != None and not fixed:
        lnames = set(aux_streams.keys())
        lnames.add('video')
        manager.add_operator(ConvertMap('map', tagger, batch_size, input_names=lnames))
    elif not fixed:
        manager.add_operator(ConvertMap('map', tagger, batch_size, input_names=['video']))
    


    manager.add_operator(ConvertSplit('crops'))
    
    manager.add_operator(PutCropOp('put', name, args, base_dir))
    
    manager.add_operator(HeaderOp('headers', conn, name))
    
    
    manager.add_operator(LabelMapOp('labels', conn, name, results = 'ids'))
    
    #manager.draw()
    results = manager.run(results = 'ids')
    
    return results