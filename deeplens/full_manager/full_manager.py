"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered.py defines file storage operations. These operations allows for
complex file storage systems such as tiered storage, and easy file
movement and access.

""" 


from deeplens.full_manager.full_videoput import *
from deeplens.pipeline import *
from deeplens.streams import *

from deeplens.utils.constants import *
from deeplens.utils.error import *
from deeplens.dataflow.feedback import *
from deeplens.full_manager.full_header_helper import *

import os
import psycopg2
import logging
from multiprocessing import Pool
import time
from deeplens.utils.utils import *
import itertools

DEFAULT_ARGS = {'frame_rate': 24, 'encoding': MP4V, 'limit': -1, 'sample': 1.0, 'offset': 0, 'batch_size': 120, 'num_processes': 4, 'background_scale': 1}

class FullStorageManager():
    """ TieredStorageManager is the implementation of a 3 tiered
    storage manager, with a cache and external storage, where cache
    is in the same location as disk, and external storage is another
    directory
    """
    def __init__(self, content_tagger, content_splitter, local_dir, remote_dir, dsn='dbname=header user=postgres password=secret host=127.0.0.1', reuse_conn=True):
        self.tagger = content_tagger
        self.splitter = content_splitter
        self.local_dir = local_dir  # the folder of video files on local disk, e.g. /var/www/html/videos
        self.remote_dir = remote_dir  # url to the video folder accessible by other machines, e.g. http://10.0.0.5/videos
        self.dsn = dsn
        self.videos = set()
        self.threads = None
        self.reuse_conn = reuse_conn

        if not os.path.exists(local_dir):
            try:
                os.makedirs(local_dir)
            except:
                raise ManagerIOError("Cannot create the directory: " + str(local_dir))


        self.conn = self.create_conn()
        self.cursor = self.conn.cursor()
        sql_create_background_table = """CREATE TABLE IF NOT EXISTS background (
                                             background_id bigint NOT NULL,
                                             clip_id bigint NOT NULL,
                                             video_name text NOT NULL,
                                             PRIMARY KEY (background_id, clip_id, video_name),
                                             FOREIGN KEY (clip_id, video_name) REFERENCES clip(clip_id, video_name) ON DELETE CASCADE
                                         );
        """
        sql_create_clip_table = """CREATE TABLE IF NOT EXISTS clip (
                                       clip_id bigint NOT NULL,
                                       video_name text NOT NULL,
                                       start_frame integer NOT NULL,
                                       end_frame integer NOT NULL,
                                       start_time integer,
                                       end_time integer,
                                       origin_x integer NOT NULL,
                                       origin_y integer NOT NULL,
                                       fwidth integer NOT NULL,
                                       fheight integer NOT NULL,
                                       width integer NOT NULL,
                                       height integer NOT NULL,
                                       video_ref text NOT NULL,
                                       is_background integer NOT NULL,
                                       translation text,
                                       PRIMARY KEY (clip_id, video_name)
                                   );
        """
        sql_create_label_table = """CREATE TABLE IF NOT EXISTS label (
                                       label text NOT NULL,
                                       value text,
                                       bbox text,
                                       clip_id bigint NOT NULL,
                                       video_name text NOT NULL,
                                       type text NOT NULL,
                                       frame integer,
                                       PRIMARY KEY (label, clip_id, video_name),
                                       FOREIGN KEY (clip_id, video_name) REFERENCES clip(clip_id, video_name) ON DELETE CASCADE
                                   );
        """
        sql_create_lineage_table = """CREATE TABLE IF NOT EXISTS lineage (
                                       video_name text NOT NULL,
                                       lineage text NOT NULL,
                                       parent text NOT NULL,
                                       PRIMARY KEY (video_name),
                                       FOREIGN KEY (video_name) REFERENCES clip(video_name) ON DELETE CASCADE
                                   );
        """
        self.cursor.execute(sql_create_label_table)
        self.cursor.execute(sql_create_background_table)
        self.cursor.execute(sql_create_clip_table)
        self.cursor.execute(sql_create_lineage_table)

    def add_table(self, name, schema = None):
        self.conn = self.create_conn()
        self.cursor = self.conn.cursor()
        if schema == None:
            schema = """CREATE TABLE IF NOT EXISTS {} (
                                       label text NOT NULL,
                                       value text,
                                       bbox text,
                                       clip_id integer NOT NULL,
                                       video_name text NOT NULL,
                                       type text NOT NULL,
                                       frames integer,
                                       PRIMARY KEY (label, clip_id, video_name, type)
                                       FOREIGN KEY (clip_id, video_name) REFERENCES clip(clip_id, video_name)
                                   );
            """.format(name)
        self.cursor.execute(schema)


    def create_conn(self):
        return psycopg2.connect(self.dsn)

    def get_conn(self):
        conn = self.conn
        if not self.reuse_conn:
            conn = self.create_conn()
        return conn

    def remove_conn(self, conn):
        if not self.reuse_conn:
            conn.commit()
            conn.close()

    def put(self, vstream, name, args=DEFAULT_ARGS, map_streams = None, aux_streams = None, fixed=False, start_time = 0):
        """put adds a video to the storage manager from a file. It should either add
            the video to disk, or a reference in disk to deep storage.
            Ags:
                vstream: VideoStream, filename, or camera input
                name: name of video in database
                args: specific arguments for inputting the datastream
                aux_streams: additional streams to correlate to video
                -> can be a dictionary of DataStreams or a dictionary of [serialized inputs, DataStream type]
                -> the second case makes sense for put_many basically
                fixed: If false, uses Tagger/Splitter Method, and if true, uses "crops" auxilary stream if it
                exists, otherwise does not crop
        """
        conn = self.get_conn()
        self.delete(name, conn)
        
        if fixed:
            tagger = None
        else:
            tagger = self.tagger
        
        write_video_single(conn, vstream, name, self.local_dir, self.remote_dir, self.splitter, tagger, args, map_streams, aux_streams, fixed, start_time)
        
        self.videos.add(name)

        self.remove_conn(conn)

    def put_many(self, vstreams, names, args=DEFAULT_ARGS, map_streams = None, aux_streams = None, fixed = False):
        conn = self.get_conn()
        start_time = time.time()
        put_args = []
        db_path = os.path.join(self.basedir, self.db_name)

        for i, v in enumerate(vstreams):
            streams = None
            if aux_streams != None:
                streams = aux_streams[i]
            if map_streams != None:
                mstreams = map_streams[i]
            #Note: streams might have to be modified to pass into threads situationally
            put_arg = (db_path, v, names[i], self.basedir, self.splitter, self.tagger, args, mstreams, streams, fixed)
            put_args.append(put_arg)
            self.delete(names[i], conn)
        
        logs = None
        with Pool(processes = args['num_processes']) as pool:
            results = pool.starmap(write_video_single, put_args)
            logs = [result[1] for result in results]

        for name in names:
            self.videos.add(name)

        #times = parallel_log_reduce(logs, start_time) -> currently not logging
        #paralleL_log_delete(logs) -> currently not deleting logs for safety
        self.remove_conn(conn)

    # def put_streams(self, name, dstreams, args=None, batch_size = -1):
    #     if args == None:
    #         args = DEFAULT_ARGS
    #     pipeline = PipelineManager(vstream)
    #     pipeline.add_operator(Materialize(name, self, args, materialize, batch_size))
    #     if dstreams != None:
    #         pipeline.add_streams(dstreams)
    #     pipeline.run(keep_result = False)

    def delete(self, name, conn = None):
        conn_not_provided = conn == None
        if conn_not_provided:
            conn = self.get_conn()
        delete_video(conn, name)
        if conn_not_provided:
            self.remove_conn(conn)

    def list(self):
        return list(self.videos)

    def size(self, name):
        """ Return the total amount of space a deeplens video takes up. Note that this doesn't include
        auxilary streams.
        """
        conn = self.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT video_ref FROM clip WHERE video_name = '%s'" % name)
        clips = cursor.fetchall()
        size = 0
        for clip in clips:
            clip = clip[0]
            try:
                size += os.path.getsize(clip)
            except FileNotFoundError:
                logging.warning("File %s not found" % clip)
        return size
    
    def get(self, query):
        """retrievies a clip of satisfying the condition.
        If the clip was in external storage, get moves it to disk. TODO: Figure out if I should implement this feature or not
        """
        conn = self.get_conn()
        c = conn.cursor()
        c.execute(query)
        result = c.fetchall()
        return result

    def get_vstreams(self, query):
        clips = self.get(query)
        refs = []
        for clip in clips:       
            if clip[0] != None:
                refs.append(clip)
        refs.sort(key=lambda tup: tup[1])
        video_refs = [refs[i][0] for i in range(len(refs))]
        clip_ids = [clip[1] for clip in refs]
        return video_refs, clip_ids

    
    def create_vstream(self, video_name, clip_id, name = 'video', stream_type = CVVideoStream):
        clip = query_clip(self.conn, clip_id, video_name)[0]
        clip_url = clip[12]
        if name == None:
            name = video_name
        origin = np.array(clip[6], clip[7])
        vstream = stream_type(clip_url, name, origin = origin, start_time = clip[2])
        return vstream

    def create_dstream(self, label, video_name, clip_id, name = None, stream_type = None):
        label = query_label_clip(self.conn, video_name, clip_id, label)[0]
        if stream_type == None:
            stream_type = label[4]
            if stream_type == 'background':
                raise TypeError('background is not a DataStream type')
            stream_type = sname_to_class(stream_type)
        if name == None:
            name = label
        dstream = stream_type(label[1], name)
        return dstream

