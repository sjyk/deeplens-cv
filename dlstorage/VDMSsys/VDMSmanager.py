"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

VDMSmanager.py defines the basic storage api in deeplens for storing in
VDMS, rather than the filesystem.
"""
from dlstorage.core import *
from dlstorage.constants import *
from dlstorage.stream import *
from dlstorage.filesystem.videoio import *
from dlstorage.header import *
from dlstorage.xform import *
from dlstorage.VDMSsys.vdmsio import *

import os

class VDMSStorageManager(StorageManager):
    #NOTE: Here, size refers to the duration of the clip, 
    #and NOT the number of frames!
    #Another NOTE: We assume that if a VDMSStorageManager instance is used to 
    #put clips in VDMS, then the same instance must be used to get() the clips
    DEFAULT_ARGS = {'encoding': H264, 'size': -1, 'limit': -1, 'sample': 1.0}
    
    def __init__(self, content_tagger):
        self.content_tagger = content_tagger
        self.clip_headers = []
        self.totalFrames = -1
    
    def put(self, filename, args=DEFAULT_ARGS):
        """In this case, put() adds the file to VDMS, along with
        the header, which we might be able to send either as a long string
        of metadata, or as extra properties (which is still a lot of metadata)
        Note: we are going to suffer the performance penalty for acquiring
        header information in a frame-by-frame fashion
        """
        v = VideoStream(filename, args['limit'])
        v = v[Sample(args['sample'])]
        v = v[self.content_tagger]
        
        if args['size'] == -1:
            tf, headers = add_video(filename, v, args['encoding'], ObjectHeader())
            self.clip_headers = headers
            self.totalFrames = tf
        else:
            tf, headers = add_video_clips(filename, v, args['encoding'], ObjectHeader(), args['size'])
            self.clip_headers = headers
            self.totalFrames = tf
    
    def get(self, name, condition, clip_size):
        """
        get() retrieves all the clips with the given name that satisfy the given condition.
        """
        return find_video(name, condition, clip_size, self.clip_headers)
        
