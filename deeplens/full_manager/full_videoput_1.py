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

import sqlite3
import random

def write_video_single(conn, video_file, target, base_dir, splitter, tagger, args, labels = None, only_labels = False, save_labels = True, background_scale=1):
    if not os.path.isfile(video_file):
        print("missing file", video_file)
        return None

    if type(conn) == str:
        conn = sqlite3.Connection(conn)
    batch_size = args['batch_size']
    
    
    manager = GraphManager()
    manager.add_stream(CVVideoStream(video_file, 'video', args['limit']), 'video')
    if labels != None:
        manager.add_streams(labels)

    manager.add_operator(ConvertMap('map', tagger, batch_size, only_labels), dstreams = {'video'})
    manager.add_operator(ConvertSplit('crops', save_labels), parents = {'map'})
    
    manager.add_operator(PutOp('put', target, args, save_labels, base_dir), parents= {'crops'}, dstreams={'video'})
    manager.add_operator(HeaderOp('headers', conn, target), parents = {'put'})
    
    if save_labels and labels is not None:
        manager.add_operator(LabelsOp('labels', conn, target), parents = {'headers'})
    
    manager.run()
    
    return results