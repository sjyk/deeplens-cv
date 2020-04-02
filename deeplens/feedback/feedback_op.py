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

    def __init__(self, storage_manager, video_stream = True, streams = 'all', batch_size = None, args = None):
        """Streams -> {'name': is_constant}
        """
        self.name = name
        self.streams = streams
        self.mat_vs = video_stream
        self.sm = storage_manager
        self.args = args
        if batch_size != None:
            self.args['batch_size'] = batch_size


    def __iter__(self):
        self.frame_iter = iter(self.video_stream)
        self.super_iter()
        self.storage = None # initialize once we start the frame
        return self

    def __next__(self):
        out = next(self.frame_iter)
        self.super_next()
        time_start = timer()
        if self.storage == None:
            if self.streams == 'all':
                self.streams = out.keys()
                if 'data' in streams:
                    streams.remove('data')
            self.storage = self.sm.put_opt(name, self.mat_vs, args = self.args)

        frame = {}
        for stream in self.streams:
            frame[stream] = out[stream]
        
        self.storage.add_frame(frame)
        
        if self.mat_vs:
            self.storage.add_frame('video', out['data'])

        self.time_elapsed += timer() - time_start

        return out

    def _serialize(self):
        return {'kernel': self.kernel,
                'threshold': self.threshold,
                'name':self.name,
                 'delay': self.delay}