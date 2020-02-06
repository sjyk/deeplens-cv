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
import json



def _update_headers_batch(conn, crops, background_id, name, video_refs,
                            full_width, full_hieght, start_time, end_time, update = False):
    """
    Update or create new headers all headers for one batch. In terms of updates, we assume certain
    constraints on the system, and only update possible changes.
    """
    if update:
        # Updates 
        for i in range(0, len(crops) + 1):
            clip_info = query_clip(conn, i + background_id, video_name)[0]
            updates = {}
            updates['start_time'] = min(start_time, clip_info[2])
            updates['end_time'] = max(end_time, clip_info[3])

            if i != 0:
                origin_x = crop[i - 1].x0
                origin_y = crop[i - 1].y0
                try:
                    translation = json.loads(clip_info[7])
                    if type(translation) is list:
                        if translation[-1][1] != origin_x or translation[-1][2] != origin_y:
                            translation.append((start_time, origin_x, origin_y))
                            updates['translation'] = translation
                    else:
                        raise ValueError('Translation object is wrongly formatted')
                except (ValueError, IndexError) as e:
                    if origin_x !=  clip_info[4] or origin_y != clip_info[5]:
                        updates['translation'] = [(start_time, origin_x, origin_y)]
                try:
                    other = json.loads(clip_info[8])
                    if type(other) is dict:
                        other.update(crops[i - 1]['all'])
                        updates['other'] = other
                    else:
                        raise ValueError('All objset is wronly formatted')
                except (ValueError, IndexError) as e: 
                    updates['other'] = crops[i-1]['all']
            update_clip_header(conn, background_id, name, updates)
    else:
        for i in range(1, len(crops) + 1):
            insert_background_header(conn, background_id, i + background_id, name)
        for i in range(0, len(crops) + 1):
            if i == 0:
                insert_clip_header(conn, i + background_id, name, 0, 0, origin_x,
                                origin_y, full_width, full_hieght, video_ref[i], is_background = True)
                
            else:
                origin_x = crop[i - 1].x0
                origin_y = crop[i - 1].y0
                width = crop[i - 1].x1 - crop[i - 1].x0
                hieght = crop[i - 1].y1 - crop[i - 1].y0
                insert_clip_header(conn, i + background_id, name, start_time, end_time, origin_x,
                                origin_y, width, hieght, video_ref[i], other = json.dump(crop[i - 1]['all']))
                
                
        for i in range(0, len(crops)):
            if type(crops['label']) is list type(crops['all']) is list:
                for j in range(len(crops['label'])):
                    insert_label_header(conn, crops['label'][j], background_id + i + 1, name)
            else:
                insert_label_header(conn, crops['label'], background_id + i + 1, name)


def _write_video_batch(vstream, \
                        crops, \
                        scratch = DEFAULT_TEMP, \
                        batch_size,
                        offset = 0, \
                        write_header = True, \
                        release = True,
                        writers = None):
    '''
    Private function which processes and stores a batch of video frames
    Arguments:
    - vstream: VideoStream which is processed
    - crops: physical crops of frames
    - batch_size: size of batch
    - release: whether we release or return the videos after finishing
    - writers: list of optional pre-existing writers that we can write frames into
    - Note: each writer must match a crop
    '''
    file_names = []
    headers = []
    header_writers = []
    if writers == None:
        r_name = get_rnd_strng()
        for i in range(len(crops) + 1):
            crop = crops[i - 1] 
            seg_name = os.path.join(scratch, r_name)
            file_name = add_ext(seg_name, AVI, seq + i)
            file_names.append(file_name)

            fourcc = cv2.VideoWriter_fourcc(*encoding)
            if i == 0:
                width = vstream.width
                height = vstream.height
            else:
                width = abs(crops[i - 1]['bb'].x1 - crops[i - 1]['bb'].x0)
                height = abs(crops[i - 1]['bb'].y1 - crops[i - 1]['bb'].y0)
            out_vid = cv2.VideoWriter(file_name,
                                    fourcc, 
                                    frame_rate, 
                                    (width, height),
                                    True)
            out_vids.append(out_vid)
            header = []
            header_writers
    else:
        out_vids = writers
    
    index = 0
    for frame in vstream:
        if len(crops) == 0:
            out_vids[0].write(frame['data'])
        else:
            out_vids[0].write(reverse_crop(frame['data'], crops))

        i = 1
        for cr in crops:
            fr = crop_box(frame['data'], cr['bb'])
            out_vids[i].write(fr)
            i +=1
        index += 1
        if index >= batch_size:
            break
    if not release:
        if len(file_names) != 0:
            return (out_vids, file_names, index)
        else:
            return (out_vids, None, index)
    else:
        for vid in out_vids:
            vid.release()
        if len(file_names) != 0:
            return (None, file_names, index)
    return (None, None, index)

def _split_video_batch(vstream, \
                        splitter, \
                        batch_size, \
                        process_vid = False, \
                        scratch = None,\
                        vstream_behind = None
                        v_cache = None):
    '''
    Private function which labels and crops a batch of video frames.
    Arguments:
    - vstream: VideoStream which is labeled
    - splitter: Splitter object which crops based on labels
    - size: size of batch
    - process_vid: whether we also process the video batch after applying a map to it
    -   Note: if this is True, we also need scratch and vstream_behind
    - scratch: where to store the video batch after processing it
    - vstream_behind: a copy of the previous video stream so that we can apply map onto it
    - v_cache: cache a buffer of the vstream (neccessary for streaming)
    '''
    labels = []
    i = 0
    for frame in vstream:
        objects.append(frame['objects'])
        i += 1
        if v_cache:
            v_cache.append(frame)
        if i >= batch_size:
            break
    limit = min(limit, i)
    crops = splitter.map(labels)
    if process_vid:
        if not splitter.map_to_video:
            raise ManagerIOError('Splitter does not support map to video')
        videos = _write_video_batch(vstream_behind, crops, limit)
        return (crops, videos)
    return crops
    

# TODO: headers and parallelize
def write_video_single(conn, \
                        video_file, \
                        output, \
                        splitter, \
                        map, \
                        scratch = DEFAULT_TEMP,\
                        limit = -1,\
                        batch_size = 100, \
                        stream = False):
    
    v = VideoStream(filename, limit)
    v = iter(v[map])
    full_width = v.width
    full_hieght = v.height
    curr_back = 0 # current clip background id
    start_time = 0 #current batch start time (NOTE: Not current clip start time)
    if stream:
        v_behind = [] # if it's a stream, we cache the buffered video instead of having a slow pointer
    else:
        v_behind = VideoStream(filename, limit)
        v_behind = iter(v)
    objects = []
    vid_files = []
    for frame in v:
        objects.append(objects['objects'])
        i += 1
        if stream:
            v_behind.append(frame)
        if i >= limit:
            break
    crops, batch_prev = splitter.initialize(objects)
    (writers, file_names, time_block) = _write_video_batch(v_behind, crops, scratch, batch_size, release = False)
    
    _update_headers_batch(conn, crops, curr_back, video_file, file_names,
                            full_width, full_hieght, start_time, start_time + time_block, update = False)
    start_time += start_time + time_block
    curr_back = curr_back + len(crops) + 1
    vid_files.extend(file_names)
    while v:
        if stream:
            v_behind = []
            v_cache = v_behind
        else:
            v_cache = None
        batch_crops = _split_video_batch(v, splitter, batch_size, v_cache = v_cache)
        crops, batch_prev, do_join = splitter.join(batch_prev, batch_crops)
        if do_join:
            writers, _ , time_block = _write_video_batch(v_behind, crops, scratch, batch_size, release = False, writers = writers)
            
            _update_headers_batch(conn, crops, curr_back, video_file, file_names,
                            full_width, full_hieght, start_time, start_time + time_block, update = True)
            start_time = start_time + time_block
        else:
            for writer in writers:
                writer.release()
            writers, file_names = _write_video_batch(v_behind, crops, scratch, batch_size, release = False)

            _update_headers_batch(conn, crops, curr_back, video_file, file_names,
                            full_width, full_hieght, start_time, start_time + time_block, update = True)
            start_time = start_time + time_block
            curr_back = curr_back + len(crops) + 1
        vid_files.extend(file_names)
    return vid_files

def delete_video_if_exists(conn, video_name):
    c = conn.cursor()
    c.execute("SELECT clip_id FROM clips WHERE video_name = '%s'" % video_name)
    clips = c.fetchall()
    if len(clips) == 0:
        # not exist in header file, nothing to do
        return

    clips = set().union(*map(set, clips))
    for clip in clips:
        c.execute("SELECT video_ref FROM clip WHERE clip_id = '%d' AND video_name = '%s'" % (clip, video_name))
        video_ref = c.fetchone()[0]
        try:
            os.remove(video_ref)
        except FileNotFoundError:
            logging.warning("File %s not found" % video_ref)
            
    c.execute("DELETE FROM clip WHERE video_name = '%s'" % (video_name))
    c.execute("DELETE FROM label WHERE video_name = '%s'" % (video_name))
    c.execute("DELETE FROM background WHERE video_name = '%s'" % video_name)
    conn.commit()


def move_one_file(conn, clip_id, video_name, dest_ref):
    c = conn.cursor()
    c.execute("SELECT video_ref FROM clip WHERE clip_id = '%d' AND video_name = '%s' " % (clip_id, video_name))
    video_ref = c.fetchone()[0]
    shutil.move(video_ref, dest_ref)
    c.execute("UPDATE clip SET video_ref = '%s' WHERE clip_id = '%d' AND video_name = '%s'" % (dest_ref, clip_id, video_name))
    conn.commit()


def insert_clip_header(conn, clip_id, video_name, start_time, end_time, origin_x, origin_y, height, width, video_ref='', is_background = False, translation = 'NULL', other = 'NULL'):
    c = conn.cursor()
    c.execute("INSERT INTO clip VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                   (clip_id, video_name, start_time, end_time, origin_x, origin_y, height, width, video_ref, is_background, translation, other))
    conn.commit()


def insert_background_header(conn, background_id, clip_id, video_name):
    c = conn.cursor()
    c.execute("INSERT INTO background VALUES (?, ?, ?)", (background_id, clip_id, video_name))
    conn.commit()

def insert_label_header(conn, label, clip_id, video_name):
    c = conn.cursor()
    c.execute("INSERT INTO label VALUES (?, ?, ?)", (label, clip_id, video_name))
    conn.commit()

def delete_label_header(conn, video_name, label = None, clip_id = None):
    c = conn.cursor()
    if label and clip_id:
        c.execute("DELETE FROM label WHERE label = '%s' AND clip_id = '%d' AND video_name = '%s' " % (label, clip_id, video_name))
    elif label:
        c.execute("DELETE FROM label WHERE label = '%s' AND video_name = '%s' " % (label, video_name))
    elif clip_id:
        c.execute("DELETE FROM label WHERE clip_id = '%d' AND video_name = '%s' " % (label, clip_id, video_name))
    else:
        raise ValueError("Neither label nor clip_id is given")

def delete_clip(conn, clip_id, video_name):
    c = conn.cursor()
    c.execute("SELECT video_ref FROM clip WHERE clip_id = '%d' AND video_name = '%s" % (clip_id, video_name))
    video_ref = c.fetchone()[0]
    try:
        os.remove(video_ref)
    except FileNotFoundError:
        logging.warning("File %s not found" % video_ref)
    c.execute("DELETE FROM clip WHERE clip_id = '%d' and video_name = '%s' " % (clip_id, video_name))
    c.execute("DELETE FROM label WHERE clip_id = '%d' and video_name = '%s' " % (clip_id, video_name))
    c.execute("DELETE FROM background WHERE clip_id = '%d' and video_name = '%s' " % (clip_id, video_name))
    conn.commit()

def delete_backgound(conn, background_id, video_name):
    c = conn.cursor()
    c.execute("SELECT clip_id FROM background WHERE background_id = '%d' AND video_name = '%s" % (background_id, video_name))
    clips = c.fetchall()
    if len(clips) == 0:
        # not exist in header file, nothing to do
        return
    clips = set().union(*map(set, clips))
    clips = clips.add(background_id)
    
    for clip in clips:
        c.execute("SELECT video_ref FROM clip WHERE clip_id = '%d' AND video_name = '%s'" % (clip, video_name))
        video_ref = c.fetchone()[0]
        try:
            os.remove(video_ref)
        except FileNotFoundError:
            logging.warning("File %s not found" % video_ref)
        c.execute("DELETE FROM clip WHERE clip_id = '%d' AND video_name = '%s'" % (clip_id, video_name))
    
    c.execute("DELETE FROM label WHERE clip_id = '%d' video_name = '%s' " % (clip_id, video_name))
    c.execute("DELETE FROM background WHERE background_id = '%d'" % background_id)
    conn.commit()


def update_clip_header(conn, clip_id, video_name, args={}):
    c = conn.cursor()
    for key, value in args.items():
        c.execute("UPDATE clip SET '%s' = '%s' WHERE clip_id = '%d' AND video_name = '%s'" % (key, value, clip_id, video_name))
    conn.commit()

def query_clip(conn, clip_id, video_name):
    c = conn.cursor()
    c.execute("SELECT * FROM clip WHERE clip_id = '%d' AND video_name = '%s" % (clip_id, video_name))
    result = c.fetchall()
    return result

def query_background(conn, video_name, background_id=None, clip_id=None):
    c = conn.cursor()
    if background_id == None and clip_id == None:
        raise ValueError("Neither background_id nor clip_id is given")
    elif background_id != None and clip_id != None:
        c.execute("SELECT * FROM background WHERE background_id = '%d' AND clip_id = '%d' AND video_name = '%s'" % (background_id, clip_id, video_name))
    elif background_id != None and clip_id == None:
        c.execute("SELECT * FROM background WHERE background_id = '%d' and video_name = '%s" % (background_id, video_name))
    elif background_id == None and clip_id != None:
        c.execute("SELECT * FROM background WHERE clip_id = '%d' and video_name = '%s" % (clip_id, video_name))
    result = c.fetchall()
    return result