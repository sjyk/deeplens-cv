"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered_videoio.py uses opencv (cv2) to read and write files to disk. It contains
primitives to encode and decode archived and regular video formats for a tiered
storage system.
"""

from deeplens.full_manager.tiered_file import *
from deeplens.constants import *
from deeplens.struct import *
from deeplens.header import *
from deeplens.utils.clip import *
from deeplens.simple_manager.file import *
from deeplens.utils.frame_xform import *

import cv2
import os
from os import path
import time
import shutil
from pathlib import Path
from datetime import datetime
import logging

# TODO: WE NEED TO COMPLETELY REWRITE THIS FILE


def write_video_auto(vstream, \
                        output, \
                        encoding, \
                        header_info,
                        output_extern = None, \
                        scratch = DEFAULT_TEMP, \
                        frame_rate=DEFAULT_FRAME_RATE, \
                        header_cmp=RAW):
    """write_video_clips takes a stream of video and writes
    it to disk. It includes the specified header 
    information as a part of the video file. The difference is that 
    it writes a video to disk/external storage from a stream in clips of a specified 
    size

    Args:
        vstream - a videostream or videotransform
        output - output file
        header_info - header information
        clip_size - how many frames in each clip
        scratch - temporary space to use
        frame_rate - the frame_rate of the video
        header_cmp - compression if any on the header
        output_extern - if the file goes to external 
        storage, specify directory
    """
    pass


#delete a video
def delete_video_if_exists(conn, video_name):
    c = conn.cursor()
    c.execute("SELECT background_id, clip_id FROM background WHERE video_name = '%s'" % video_name)
    clips = c.fetchall()
    if len(clips) == 0:
        # not exist in header file, nothing to do
        return

    clips = set().union(*map(set, clips))
    for clip in clips:
        c.execute("SELECT video_ref FROM clip WHERE clip_id = '%d'" % clip)
        video_ref = c.fetchone()[0]
        try:
            os.remove(video_ref)
        except FileNotFoundError:
            logging.warning("File %s not found" % video_ref)
        c.execute("DELETE FROM clip WHERE clip_id = '%d'" % clip)

    c.execute("DELETE FROM background WHERE video_name = '%s'" % video_name)
    conn.commit()


def move_one_file(conn, clip_id, dest_ref):
    c = conn.cursor()
    c.execute("SELECT video_ref FROM clip WHERE clip_id = '%d'" % clip_id)
    video_ref = c.fetchone()[0]
    shutil.move(video_ref, dest_ref)
    c.execute("UPDATE clip SET video_ref = '%s' WHERE clip_id = '%d'" % (dest_ref, clip_id))
    conn.commit()


def insert_clip(conn, clip_id, start_time, end_time, origin_x, origin_y, height, width,
                has_label, labels='', video_ref=''):
    c = conn.cursor()
    c.execute("INSERT INTO clip VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (clip_id, start_time, end_time, origin_x, origin_y, height, width, has_label, labels, video_ref))
    conn.commit()


def insert_background(conn, background_id, clip_id, video_name):
    c = conn.cursor()
    c.execute("INSERT INTO background VALUES (?, ?, ?)", (background_id, clip_id, video_name))
    conn.commit()


def delete_clip(conn, clip_id):
    c = conn.cursor()
    c.execute("SELECT video_ref FROM clip WHERE clip_id = '%d'" % clip_id)
    video_ref = c.fetchone()[0]
    try:
        os.remove(video_ref)
    except FileNotFoundError:
        logging.warning("File %s not found" % video_ref)
    c.execute("DELETE FROM clip WHERE clip_id = '%d'" % clip_id)
    conn.commit()


def update_clip(conn, clip_id, args={}):
    c = conn.cursor()
    for key, value in args.items():
        c.execute("UPDATE clip SET '%s' = '%s' WHERE clip_id = '%d'" % (key, value, clip_id))
    conn.commit()


def query_clip(conn, clip_id):
    c = conn.cursor()
    c.execute("SELECT * FROM clip WHERE clip_id = '%d'" % clip_id)
    result = c.fetchall()
    return result


def query_background(conn, background_id=None, clip_id=None):
    c = conn.cursor()
    if background_id == None and clip_id == None:
        raise ValueError("Neither background_id nor clip_id is given")
    elif background_id != None and clip_id != None:
        c.execute("SELECT * FROM background WHERE background_id = '%d' AND clip_id = '%d'" % (background_id, clip_id))
    elif background_id != None and clip_id == None:
        c.execute("SELECT * FROM background WHERE background_id = '%d'" % background_id)
    elif background_id == None and clip_id != None:
        c.execute("SELECT * FROM background WHERE clip_id = '%d'" % clip_id)
    result = c.fetchall()
    return result
