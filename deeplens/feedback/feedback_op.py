"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

storage_feedback.py defines functions related to the materialization of the
pipeline.

"""

import cv2
import json

## TODO: add a stream to a previous VideoStream??

class Materialize(Operator):
    """Filter() defines cross-correlation kernel and a threshold. It
    slides this kernel across the metric and if this threshold is exceeded
    it defines an event {True, False} variable.
    """

    def __init__(self, name, storage_manager, args = None, materialize = True, streams = 'all', batch_size = -1):
        self.streams = streams
        self.materialize = materialize
        self.sm = storage_manager
        self.args = args
        self.batch_size = batch_size
        if batch_size == None:
            self.batch_size = args['batch_size']
        self.conn = self.sm.conn
        self.dir =  self.sm.base_dir
        self.writers = {}

    def __iter__(self):
        self.pipeline = iter(self.pipeline)
        self.batch_index = 0
        return self

    def _create_writers(self, data):
        if self.materialize:
            r_name = vid_name + get_rnd_strng(64)
            seg_name = os.path.join(self.dir, r_name)
            self.file_name = add_ext(seg_name, AVI)
            self.writers['video'] = data['video'].init_mat(self.file_name, fourcc, data['video'].width, 
                                                            data['video'].hieght, self.args['frame_rate'])
        
        for stream in self.streams:   
            self.writers[stream] = data[stream].init_mat()

    def _materialize_streams(self):
        # update header for video -> can we have an empty clip? -> yes?
        if self.materialize:
            video_ref = self.file_name
        else:
            video_ref = None
        clip_id = random.getrandbits(63) #update
        insert_clip_header(self.conn, self.name, 
                            self.start_time, self.end_time, self.origin[0], self.origin[1], 
                            self.height, self.width, video_ref)
        for stream in self.streams:
            if type(data[stream]) == JSONListDataStream:
                r_name = vid_name + get_rnd_strng(64)
                seg_name = os.path.join(self.dir, r_name)
                file_name = add_ext(seg_name, AVI)
                with open(file_name, 'w') as f:
                    data[stream].materialize(self.writers[stream], f)
                info = file_name
            else:
                info = data[stream].materialize(self.writers[stream])
            # update label with header
            stream_type = type(data[stream]).__name__
            insert_label_header(self.conn, stream, info, clip_id, self.name, stream_type)
            
    def __next__(self):
        try:
            data = next(self.pipeline)
        except:
            self._materialize_streams()
            raise StopIteration() 
        if streams == 'all':
            streams = data.keys()
        if 'video' in streams:
            streams.remove('video')
        if self.batch_index == 0:
            self._create_writers(self, data)
            self.start_time = data['video'].start_time
            self.width = data['video'].width
            self.height = data['video'].height
            self.origin = data['video'].origin
        
        elif self.height != data['video'].height or self.width != data['video'.width]:
            self._materialize_streams()
            self._create_writers(self, data)
            self.start_time = data['video'].start_time
            self.width = data['video'].width
            self.height = data['video'].height
            self.origin = data['video'].origin
            self.batch_index = 0
        
        if self.materialize:
            self.writers['video'] = data['video'].append(data['video'].get(), self.writers['video'])
        
        self.end_time = data['video'].frame_count + self.start_time
        for stream in self.streams:            
            self.writers[stream] = data[stream].append(data[stream].get(), self.self.writers[stream])

        self.batch_index += 1
        if self.batch_index == self.batch_size:
            self._materialize_streams()
            self.writers = {}
            self.batch_index = 0

        return data

    def _serialize(self):
        return None