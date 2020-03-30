"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered.py defines file storage operations. These operations allows for
complex file storage systems such as tiered storage, and easy file
movement and access.

""" 


from deeplens.core import StorageManager
from deeplens.full_manager.full_videoio import *

from deeplens.constants import *
from deeplens.error import *

import os
import sqlite3
import logging
from multiprocessing import Pool
import time
from deeplens.utils.parallel_log_reduce import *

DEFAULT_ARGS = {'encoding': MP4V, 'limit': -1, 'sample': 1.0, 'offset': 0, 'batch_size': 20, 'num_processes': 4, 'background_scale': 1}

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
                                             FOREIGN KEY (background_id, clip_id, video_name) REFERENCES clip(clip_id, clip_id, video_name)
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
                                       FOREIGN KEY (clip_id, video_name) REFERENCES clip(clip_id, video_name)
                                   );
        """
        self.cursor.execute(sql_create_label_table)
        self.cursor.execute(sql_create_background_table)
        self.cursor.execute(sql_create_clip_table)

    def put(self, filename, target, args=DEFAULT_ARGS, in_extern_storage=False, parallel=False):
        """put adds a video to the storage manager from a file. It should either add
            the video to disk, or a reference in disk to deep storage.
        """
        self.delete(target)
        if in_extern_storage: 
            physical_dir = self.externdir
        else:
            physical_dir = self.basedir
        
        if type(filename) == int:
            stream = True
        else:
            stream = False
        if self.content_tagger == None:
                tagger = filename
        else:
            tagger = self.content_tagger


        if tagger.batch_size < args['batch_size']:
            raise ValueError("This setting may currently lead to bugs")
        
        if parallel and not stream:
            db_path = os.path.join(self.basedir, self.db_name)
            write_video_parrallel(db_path, filename, target, physical_dir, self.content_splitter, tagger, args=args)
        
        else:
            write_video_single(self.conn, filename, target, physical_dir, self.content_splitter, tagger, stream=stream, args=args, background_scale=args['background_scale'])
        
        self.videos.add(target)
    
    def put_many(self, filenames, targets, args=DEFAULT_ARGS, in_extern_storage=False, log=False):
        start_time = time.time()
        put_args = []
        db_path = os.path.join(self.basedir, self.db_name)
        if in_extern_storage: 
            physical_dir = self.externdir
        else:
            physical_dir = self.basedir
        for i, name in enumerate(filenames):
            if self.content_tagger == None:
                tagger = name
            else:
                tagger = self.content_tagger
            put_arg = (db_path, name, targets[i], physical_dir, self.content_splitter, tagger, 0, False, args, log, args['background_scale'])
            put_args.append(put_arg)
            self.delete(targets[i])
        
        logs = []
        with Pool(processes = args['num_processes']) as pool:
            results = pool.starmap(write_video_single, put_args)
            for result in results:
                if result == None:
                    continue
                logs.append(result[1])

        for target in targets:
            self.videos.add(target)

        times = paralleL_log_reduce(logs, start_time)
        #paralleL_log_delete(logs) -> currently not deleting logs for safety
        return times
        

    def put_fixed(self, filename, target, crops, batch = False, args=DEFAULT_ARGS, in_extern_storage = False):
        self.delete(target)
        if in_extern_storage: 
            physical_dir = self.externdir
        else:
            physical_dir = self.basedir
        write_video_fixed(self.conn, filename, target, physical_dir, crops, batch = batch, args=args)
        self.videos.add(target)

    def get(self, name, condition):
        """retrievies a clip of satisfying the condition.
        If the clip was in external storage, get moves it to disk. TODO: Figure out if I should implement this feature or not
        """
        # TODO: This should be done by looking up SQLite database
        # if name not in self.videos:
        #     raise VideoNotFound(name + " not found in " + str(self.videos))
        logging.info("Calling get()")
        return query(self.conn, name, clip_condition = condition)
    
    def delete(self, name):
        delete_video(self.conn, name)


    def list(self):
        return list(self.videos)

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
    