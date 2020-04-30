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
    def __init__(self, content_tagger, content_splitter, basedir, db_name='header.db', reuse_conn = True):
        self.content_tagger = content_tagger
        self.content_splitter = content_splitter
        self.basedir = basedir
        self.videos = set()
        self.threads = None
        self.db_name =  db_name
        self.reuse_conn = reuse_conn

        if not os.path.exists(basedir):
            try:
                os.makedirs(basedir)
            except:
                raise ManagerIOError("Cannot create the directory: " + str(basedir))


        self.conn = self.create_conn()
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

    def create_conn(self):
        return sqlite3.connect(os.path.join(self.basedir, self.db_name))

    def get_conn(self):
        conn = self.conn
        if not self.reuse_conn:
            conn = self.create_conn()
        return conn

    def remove_conn(self, conn):
        if not self.reuse_conn:
            conn.commit()
            conn.close()

    def put(self, filename, target, args=DEFAULT_ARGS, in_extern_storage=False, parallel=False):
        """put adds a video to the storage manager from a file. It should either add
            the video to disk, or a reference in disk to deep storage.
        """
        conn = self.get_conn()
        self.delete(target, conn)
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
            write_video_parallel(db_path, filename, target, physical_dir, self.content_splitter, tagger, args=args)
        
        else:
            write_video_single(conn, filename, target, physical_dir, self.content_splitter, tagger, stream=stream, args=args, background_scale=args['background_scale'])
        
        self.videos.add(target)

        self.remove_conn(conn)

    def put_many(self, filenames, targets, args=DEFAULT_ARGS, in_extern_storage=False, log=False):
        conn = self.get_conn()
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
            self.delete(targets[i], conn)
        
        logs = []
        with Pool(processes = args['num_processes']) as pool:
            results = pool.starmap(write_video_single, put_args)
            for result in results:
                if result == None:
                    continue
                logs.append(result[1])

        for target in targets:
            self.videos.add(target)

        times = parallel_log_reduce(logs, start_time)
        #paralleL_log_delete(logs) -> currently not deleting logs for safety
        self.remove_conn(conn)
        return times


    def put_fixed(self, filename, target, crops, batch = False, args=DEFAULT_ARGS, in_extern_storage = False):
        conn = self.get_conn()
        self.delete(target, conn)
        if in_extern_storage: 
            physical_dir = self.externdir
        else:
            physical_dir = self.basedir
        write_video_fixed(conn, filename, target, physical_dir, crops, batch = batch, args=args)
        self.videos.add(target)
        self.remove_conn(conn)

    def get(self, name, condition):
        """retrievies a clip of satisfying the condition.
        If the clip was in external storage, get moves it to disk. TODO: Figure out if I should implement this feature or not
        """
        # TODO: This should be done by looking up SQLite database
        # if name not in self.videos:
        #     raise VideoNotFound(name + " not found in " + str(self.videos))
        conn = self.get_conn()
        logging.info("Calling get()")
        return query(self.conn, name, clip_condition = condition)


    def cache(self, name, condition):
        """Caches the specified clips as pre-decoded files
        """
        logging.info("Calling cache()")
        return cache(self.conn, name, clip_condition = condition)
    
    def uncache(self, name, condition):
        """Removes the specified clips as pre-decoded files
        """
        logging.info("Calling uncache()")
        return uncache(self.conn, name, clip_condition = condition)
    

    def delete(self, name, conn=None):
        conn_not_provided = conn == None
        if conn_not_provided:
            conn = self.get_conn()

        delete_video(conn, name)

        if conn_not_provided:
            self.remove_conn(conn)

    def list(self):
        return list(self.videos)

    def size(self, name):
        """ Return the total amount of space a deeplens video takes up
        """
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT background_id, clip_id FROM background WHERE video_name = '%s'" % name)
        clips = cursor.fetchall()
        clips = set().union(*map(set, clips))
        size = 0
        for clip in clips:
            cursor.execute("SELECT video_ref FROM clip WHERE clip_id = '%d'" % clip)
            video_ref = cursor.fetchone()[0]
            try:
                size += os.path.getsize(video_ref)
            except FileNotFoundError:
                logging.warning("File %s not found" % video_ref)

        return size
    