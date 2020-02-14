"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered.py defines file storage operations. These operations allows for
complex file storage systems such as tiered storage, and easy file
movement and access.

""" 


#TODO: Note, we ignored multi-threading for now
from deeplens.core import StorageManager
from deeplens.full_manager.full_videoio import *

from deeplens.constants import *
from deeplens.struct import *
from deeplens.dataflow.map import Sample
from deeplens.header import *
from deeplens.error import *

import os
import sqlite3
import logging

DEFAULT_ARGS = {'encoding': MP4V, 'size': -1, 'limit': -1, 'sample': 1.0, 'offset': 0, 'batch_size': 20}

# NOTE: bounding boxes are at a clip level

class FullStorageManager(StorageManager):
    """ TieredStorageManager is the implementation of a 3 tiered
    storage manager, with a cache and external storage, where cache
    is in the same location as disk, and external storage is another
    directory
    """
    def __init__(self, content_tagger, content_splitter, basedir, db_name='header.db'):
        self.content_tagger = content_tagger
        self.content_splitter = content_splitter
        self.basedir = basedir
        self.videos = set()
        self.threads = None
        self.STORAGE_BLOCK_SIZE = 60 
        self.db_name =  db_name

        if not os.path.exists(basedir):
            try:
                os.makedirs(basedir)
            except:
                raise ManagerIOError("Cannot create the directory: " + str(basedir))


        self.conn = sqlite3.connect(os.path.join(basedir, db_name))
        self.cursor = self.conn.cursor()
        sql_create_background_table = """CREATE TABLE IF NOT EXISTS background (
                                             background_id integer NOT NULL,
                                             clip_id integer NOT NULL,
                                             video_name text NOT NULL,
                                             PRIMARY KEY (background_id, clip_id, video_name)
                                         );
        """
        sql_create_clip_table = """CREATE TABLE IF NOT EXISTS clip (
                                       clip_id integer NOT NULL,
                                       video_name text NOT NULL,
                                       start_time integer NOT NULL,
                                       end_time integer NOT NULL,
                                       origin_x integer NOT NULL,
                                       origin_y integer NOT NULL,
                                       height integer NOT NULL,
                                       width integer NOT NULL,
                                       video_ref text NOT NULL,
                                       is_background NOT NULL,
                                       translation text,
                                       other text,
                                       PRIMARY KEY (clip_id, video_name)
                                   );
        """
        sql_create_label_table = """CREATE TABLE IF NOT EXISTS label (
                                       label text NOT NULL,
                                       clip_id integer NOT NULL,
                                       video_name text NOT NULL,
                                       PRIMARY KEY (label, clip_id, video_name)
                                   );
        """
        self.cursor.execute(sql_create_label_table)
        self.cursor.execute(sql_create_background_table)
        self.cursor.execute(sql_create_clip_table)

    def put(self, filename, target, args=DEFAULT_ARGS, in_extern_storage = False):
        """put adds a video to the storage manager from a file. It should either add
            the video to disk, or a reference in disk to deep storage.
        """
        #delete_video_if_exists(physical_clip) TODO: Update function
        if in_extern_storage: 
            physical_dir = self.externdir
        else:
            physical_dir = self.basedir
        

        write_video_single(self.conn, filename, target, physical_dir, self.content_splitter, self.content_tagger, args=args)
        
        self.videos.add(target)

    def get(self, name, label, condition = None):
        """retrievies a clip of satisfying the condition.
        If the clip was in external storage, get moves it to disk. TODO: Figure out if I should implement this feature or not
        """
        if name not in self.videos:
            raise VideoNotFound(name + " not found in " + str(self.videos))

        return query(self.conn, name, label, clip_condition = condition)
    
    def delete(self, name):
        delete_video(self.conn, name)


    def list(self):
        return list(self.videos)
    
    #TODO implement after we support threading
    def setThreadPool(self):
        raise NotImplementedError("This storage manager does not support threading")

    def size(self, name):
        """ Return the total amount of space a deeplens video takes up
        """

        self.cursor.execute("SELECT background_id, clip_id FROM background WHERE video_name = '%s'" % name)
        clips = self.cursor.fetchall()
        clips = set().union(*map(set, clips))
        size = 0
        for clip in clips:
            self.cursor.execute("SELECT video_ref FROM clip WHERE clip_id = '%d'" % clip)
            video_ref = self.cursor.fetchone()[0]
            try:
                size += os.path.getsize(video_ref)
            except FileNotFoundError:
                logging.warning("File %s not found" % video_ref)

        return size
    
    #TODO: We should make the storage distributed instead
    def moveToExtern(self, name, condition): 
        """ Move clips that fulfil the condition to disk
        """
        extern_dir = os.path.join(self.externdir, name)
        physical_clip = os.path.join(self.basedir, name)
        return move_to_extern_if(physical_clip, condition, extern_dir)

    def moveFromExtern(self, name, condition): 
        """ Move clips that fulfil the condition to disk
        """
        physical_clip = os.path.join(self.basedir, name)
        move_from_extern_if(physical_clip, condition, threads=None)

    def isExtern(self, name, condition):
        """ Default: returns True if any of the files that meet the requirements
        is in external storage
        """
        physical_clip = os.path.join(self.basedir, name)
        return check_extern_if(physical_clip, condition, threads=None)