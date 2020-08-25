"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered_videoio.py uses opencv (cv2) to read and write files to disk. It contains
primitives to encode and decode archived and regular video formats for a tiered
storage system.
"""

from deeplens.header import *
from deeplens.dataflow.map import Resize
from deeplens.simple_manager.file import *
from deeplens.utils.frame_xform import *
from deeplens.extern.ffmpeg import *
from deeplens.extern.cache import *
from deeplens.media.youtube_tagger import *
from deeplens.struct import *

import psycopg2
import random

import cv2
import os
from os import path
import time
import shutil
import logging
import json
import itertools
#import threading
#import queue
from multiprocessing import Pool
#import glob

def _update_headers_batch(conn, crops, name, start_time, end_time, ids):

    for i, id in enumerate(ids):
        clip_info = query_clip(conn, id, name)[0]
        updates = {}
        updates['start_time'] = min(start_time, clip_info[2])
        updates['end_time'] = max(end_time, clip_info[3])
        if i != 0:
            origin_x = crops[i - 1]['bb'].x0
            origin_y = crops[i - 1]['bb'].y0
            translation = clip_info[10]
            if translation == 'NULL':
                if origin_x != clip_info[4] or origin_y != clip_info[5]:
                    updates['translation'] = json.dumps([(start_time, origin_x, origin_y)])
            else:
                translation = json.loads(clip_info[10])
                if type(translation) is list:
                    if translation[-1][1] != origin_x or translation[-1][2] != origin_y:
                        translation.append((start_time, origin_x, origin_y))
                        updates['translation'] = json.dumps(translation)
                else:
                    raise ValueError('Translation object is wrongly formatted')
            other = clip_info[11]
            if other == 'NULL':
                updates['other'] = json.dumps(crops[i - 1]['all'], cls=Serializer)
            else:
                other = json.loads(clip_info[11])
                if type(other) is dict:
                    logging.debug(crops[i - 1])
                    other.update(crops[i - 1]['all'])
                    updates['other'] = json.dumps(other, cls=Serializer)
                else:
                    raise ValueError('All object is wrongly formatted')

        update_clip_header(conn, id, name, updates)


def _new_headers_batch(conn, all_crops, name, video_refs,
                            full_width, full_height, start_time, end_time, ids = None):
    """
    Create new headers all headers for one batch. In terms of updates, we assume certain
    constraints on the system, and only update possible changes.
    """
    crops = all_crops[0]
    ids = [random.getrandbits(63) for i in range(len(crops) + 1)]
    for i in range(0, len(crops) + 1):
        if i == 0:
            if len(crops):
                insert_clip_header(conn, ids[0], name, start_time, end_time, 0, 0,
                                full_width, full_height, video_refs[i], is_background=True)
            else:
                insert_clip_header(conn, ids[0], name, start_time, end_time, 0, 0,
                                full_width, full_height, video_refs[i], is_background=False)
        else:
            origin_x = crops[i - 1]['bb'].x0
            origin_y = crops[i - 1]['bb'].y0
            width = crops[i - 1]['bb'].x1 - crops[i - 1]['bb'].x0
            height = crops[i - 1]['bb'].y1 - crops[i - 1]['bb'].y0
            insert_clip_header(conn, ids[i], name, start_time, end_time, origin_x,
                            origin_y, width, height, video_refs[i], other = json.dumps(crops[i - 1]['all'], cls=Serializer))
    for i in range(1, len(crops) + 1):
        insert_background_header(conn, ids[0], ids[i], name)

    for i in range(0, len(crops)):
        if type(crops[i]['label']) is list: # TODO: deal with crop all later
            for j in range(len(crops[i]['label'])):
                insert_label_header(conn, crops[i]['label'][j], ids[i + 1], name)
        else:
            insert_label_header(conn, crops[i]['label'], ids[i + 1], name)

    for i in range(1, len(all_crops)):
        _update_headers_batch(conn, all_crops[i], name, start_time, end_time, ids)

    return ids

def _normalize_crops(all_crops):
    batches = {}

    for crops in all_crops:
        for id, cr in enumerate(crops):
            if not id in batches:
                batches[id] = cr['bb']
            else:
                batches[id] = batches[id].union_box(cr['bb'])

    for crops in all_crops:
        for id, cr in enumerate(crops):
            cr['bb'] = batches[id]

    #print(batches[id])

    return all_crops


def _write_video_batch(vstream, \
                        vid_name, \
                        all_crops, \
                        encoding,
                        batch_size,
                        dir=DEFAULT_TEMP, \
                        frame_rate=24,
                        release=True,
                        writers=None):
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
    out_vids = []
    num_batch = len(all_crops)

    all_crops = _normalize_crops(all_crops)

    crops = all_crops[0]
    #print(crops)

    if writers == None:
        r_name = vid_name + get_rnd_strng(64)
        for i in range(len(crops) + 1):
            seg_name = os.path.join(dir, r_name)
            file_name = add_ext(seg_name, '.avi', i)
            file_names.append(file_name)

            fourcc = cv2.VideoWriter_fourcc(*encoding)
            if i == 0:
                try:
                    width = vstream.width
                    height = vstream.height
                except AttributeError:
                    width = vstream[0].shape[1]
                    height = vstream[0].shape[0]

                out_vid = cv2.VideoWriter(file_name,
                                          fourcc,
                                          frame_rate,
                                          (width, height),
                                          True)
            else:
                width = abs(crops[i - 1]['bb'].x1 - crops[i - 1]['bb'].x0)
                height = abs(crops[i - 1]['bb'].y1 - crops[i - 1]['bb'].y0)
                out_vid = cv2.VideoWriter(file_name,
                                          fourcc,
                                          frame_rate,
                                          (width, height),
                                          True)
            out_vids.append(out_vid)
    else:
        out_vids = writers
    index = 0
    j = 0

    vid_write_count = {k:set() for k in range(len(out_vids))}

    for frame in vstream:
        cnt = frame['frame']
        if type(frame) == dict:
            frame = frame['data']
        newX, newY = frame.shape[1], frame.shape[0]
        if len(crops) == 0:
            out_vids[0].write(cv2.resize(frame, (int(newX), int(newY))))
        else:
            out_vids[0].write(cv2.resize(reverse_crop(frame, crops), (int(newX), int(newY))))

            vid_write_count[0].add(reverse_crop(frame, crops).shape)

        i = 1
        for cr in crops:
            fr = crop_box(frame, cr['bb'])
            #print(cnt, fr.shape, cr['bb'])
            out_vids[i].write(fr)
            vid_write_count[i].add(fr.shape)
            i +=1

        index += 1

        if index >= num_batch*batch_size :
            #print('break',cnt, index)
            break

        if index % batch_size == 0:
            j += 1
            crops = all_crops[j]

    # the vstream iterator was closed before finishing the batch
    if index < num_batch * batch_size:
        vstream.finished = True

    #print(num_batch, batch_size, index, vid_write_count)

    if not release:
        if len(file_names) != 0:
            return (out_vids, file_names, index)
        else:
            return (out_vids, None, index)
    else:
        if len(file_names) != 0:
            return (None, file_names, index)
    return (None, None, index)

def _split_video_batch(vstream,
                        splitter,
                        batch_size,
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
    - v_cache: cache a buffer of the vstream (neccessary for streaming)
    '''
    labels = []
    i = 0
    for frame in vstream:
        labels.append(frame['objects'])
        i += 1
        if v_cache != None:
            v_cache.append(frame['frame'])
        if i >= batch_size and batch_size != -1:
            break
    if i == 0:
        return None
    crops = splitter.map(labels)
    return crops
    
def write_video_single(conn, \
                        video_file, \
                        target, \
                        dir, \
                        splitter, \
                        map, \
                        start_time=0, \
                        stream=False, \
                        args={}, 
                        log=False,
                        background_scale=1,
                        rows=None,
                        hwang=False):
    start = time.time()
    if type(map) == str:
        map = YoutubeTagger(map, './deeplens/media/train/processed1.csv')
    if type(conn) == str:
        conn = psycopg2.connect(conn)
    batch_size = args['batch_size']

    v = VideoStream(video_file, args['limit'], rows=rows, hwang=hwang)#[Resize(0.99)]
    v = iter(v[map])
    if stream:
        try:
            v.set_stream(True)
        except NameError:
            logging.warning("set_stream isn't a function in map")
    full_width = v.width
    full_height = v.height
    start_time = start_time #current batch start time
    all_crops = []
    i = 0
    if stream:
        v_behind = [] # if it's a stream, we cache the buffered video instead of having a slow pointer
    else:
        v_behind = VideoStream(video_file, args['limit'], rows=rows, hwang=hwang)#[Resize(0.99)]
        v_behind = iter(v_behind)
    labels = []
    vid_files = []

    for frame in v:
        labels.append(frame['objects'])
        logging.debug(labels)
        i += 1
        if stream:
            v_behind.append(frame['frame'])
        if i >= batch_size:
            break
    crops, batch_prev, _ = splitter.initialize(labels)
    #(writers, file_names, time_block) = _write_video_batch(v_behind, target, crops, args['encoding'], batch_size, dir = dir, release = False)
    #ids = _update_headers_batch(conn, crops, target, file_names,
    #                        full_width, full_height, start_time, start_time + time_block, update = False)
    #start_time = start_time + time_block
    #vid_files.extend(file_names)
    all_crops.append(crops)

    #print('A',[1 for c in crops if c['bb'].y1 > 1080])

    time_block = 0
    i = 0
    while True:
        if stream:
            v_behind = []
            v_cache = v_behind
        else:
            v_cache = None
        batch_crops = _split_video_batch(v, splitter, batch_size, v_cache = v_cache)
        if batch_crops == None:
            break
        crops, batch_prev, do_join = splitter.join(batch_prev, batch_crops)
        
        #print('B',[c['bb'] for c in crops if c['bb'].y1 > 1080])
        #print(crops)
        #if do_join:
            #writers, _ , time_block = _write_video_batch(v_behind, target, crops, args['encoding'], batch_size, dir, release = False, writers = writers)
            #_update_headers_batch(conn, crops, target, file_names,
            #                full_width, full_height, start_time, start_time + time_block, ids = ids, update = True)
            #start_time = start_time + time_block
            #all_crops.append(crops)
        if not do_join:
            #print('hello' + str(i))
            _, file_names, time_block = _write_video_batch(v_behind, target, all_crops, args['encoding'], batch_size, dir, release=True)
            if time_block == 0:
                break
            #print(file_names, time_block)
            ids = _new_headers_batch(conn, all_crops, target, file_names,
                            full_width, full_height, start_time, start_time + time_block)
            start_time = start_time + time_block
            vid_files.extend(file_names)
            all_crops =[]
        all_crops.append(crops)
        i +=1
    _, file_names, time_block = _write_video_batch(v_behind, target, all_crops, args['encoding'], batch_size, dir, release=True)
    ids = _new_headers_batch(conn, all_crops, target, file_names,
                full_width, full_height, start_time, start_time + time_block)
    vid_files.extend(file_names)
    end = time.time()
    log_info = {}
    log_info['video_id'] = target
    log_info['video_file'] = video_file
    log_info['duration'] = end - start
    log_info['end_time'] = end

    logging.info(json.dumps(log_info))
    if log:
        log_file = get_rnd_strng() + '.txt'
        log_file = os.path.join(dir, log_file)
        with open(log_file, 'w') as f:
            json.dump(log_info, f)
        return vid_files, log_file
    # conn.close()  # don't close the database before we finish get()!
    return vid_files
    
def write_video_parallel(db_path, \
                        video_file, \
                        target,
                        dir, \
                        splitter, \
                        mapper, \
                        scratch = DEFAULT_TEMP, \
                        args={}):
    '''
    put function with parallization
    Note: The map/join primitive doesn't completely hold in the parallel case
    Note: The limit is approximate (a more accurate/slower limit can be implemented if needed)
    '''
    cap = cv2.VideoCapture(video_file)
    fps = cap.get(cv2.CAP_PROP_FPS)
    temp_ext = os.path.splitext(video_file)[1]
    num_processes = args['num_processes']
    if args['limit'] != -1:
        end_time = float(args['limit'])/fps
        video_file = approx_etrim_video(video_file, end_time, scratch = scratch, ext = temp_ext)
    single_argss = []
    
    temp_path = split_by_key_frames(video_file, scratch = DEFAULT_TEMP, ext = temp_ext)
    temp_names = []
    i = 0
    start_time = 0
    while True:
        vid_path = temp_path %i
        if not os.path.exists(vid_path):
            break
        single_args = (db_path, vid_path, target, dir, splitter, mapper, start_time, False, args)
        duration = get_duration(vid_path)
        duration = int(duration*fps)
        start_time += duration
        single_argss.append(single_args)
        i += 1
    
    pool = Pool(processes=num_processes)
    pool.starmap(write_video_single, single_argss)
    pool.close()
    
def write_video_fixed(conn, \
                    video_file, \
                    target,
                    dir, \
                    crops, \
                    start_time = 0, \
                    batch = False, \
                    args={}):
    
    if type(conn) == str:
        conn = psycopg2.connect(conn)
    v = VideoStream(video_file, args['limit'])
    v = iter(v)
    full_width = v.width
    full_height = v.height
    start_time = start_time #current batch start time
    if not batch:
        (_ , file_names, time_block) = _write_video_batch(v, target, crops, args['encoding'], -1 , dir = dir, release = True)
        _update_headers_batch(conn, crops, target, file_names,
                            full_width, full_height, start_time, start_time + time_block, update = False)
        return file_names
    
    vid_files = []
    batch_size = args['batch_size']
    while True:
        _, file_names, time_block = _write_video_batch(v, target, crops, args['encoding'], batch_size, dir, release = True)
        if time_block == 0:
            break
        _update_headers_batch(conn, crops, target, file_names,
                        full_width, full_height, start_time, start_time + time_block, update = False)
        start_time = start_time + time_block
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
    c.execute("INSERT INTO clip VALUES ('%d', '%s', '%d', '%d', '%d', '%d', '%d', '%d', '%s', '%d', '%s', '%s')" %
               (clip_id, video_name, start_time, end_time, origin_x, origin_y, height, width, video_ref, is_background, translation, other))
    conn.commit()


def insert_background_header(conn, background_id, clip_id, video_name):
    c = conn.cursor()
    c.execute("INSERT INTO background VALUES ('%d', '%d', '%s')" % (background_id, clip_id, video_name))
    conn.commit()

def insert_label_header(conn, label, clip_id, video_name):
    c = conn.cursor()
    c.execute("INSERT INTO label VALUES ('%s', '%d', '%s')" % (label, clip_id, video_name))
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
    c.execute("SELECT video_ref FROM clip WHERE clip_id = '%d' AND video_name = '%s'" % (clip_id, video_name))
    video_ref = c.fetchone()[0]
    try:
        os.remove(video_ref)
    except FileNotFoundError:
        logging.warning("File %s not found" % video_ref)
    c.execute("DELETE FROM clip WHERE clip_id = '%d' and video_name = '%s' " % (clip_id, video_name))
    c.execute("DELETE FROM label WHERE clip_id = '%d' and video_name = '%s' " % (clip_id, video_name))
    c.execute("DELETE FROM background WHERE clip_id = '%d' and video_name = '%s' " % (clip_id, video_name))
    c.execute("DELETE FROM background WHERE background_id = '%d' and video_name = '%s' " % (clip_id, video_name))
    conn.commit()

def delete_video(conn, video_name):
    c = conn.cursor()
    c.execute("SELECT video_ref FROM clip WHERE video_name = '%s'" % (video_name))
    video_refs = c.fetchall()
    for ref in video_refs:
        try:
            os.remove(ref[0])
        except FileNotFoundError:
            logging.warning("File %s not found" % ref)
    c.execute("DELETE FROM clip WHERE video_name = '%s' " % video_name)
    c.execute("DELETE FROM label WHERE video_name = '%s' " % video_name)
    c.execute("DELETE FROM background WHERE video_name = '%s' " % video_name)
    conn.commit()

def delete_background(conn, background_id, video_name):
    c = conn.cursor()
    c.execute("SELECT clip_id FROM background WHERE background_id = '%d' AND video_name = '%s'" % (background_id, video_name))
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
        c.execute("DELETE FROM clip WHERE clip_id = '%d' AND video_name = '%s'" % (clip, video_name))
    
    c.execute("DELETE FROM label WHERE clip_id = '%d' video_name = '%s' " % (background_id, video_name))
    c.execute("DELETE FROM background WHERE background_id = '%d'" % background_id)
    conn.commit()


def update_clip_header(conn, clip_id, video_name, args={}):
    c = conn.cursor()
    for key, value in args.items():
        c.execute("UPDATE clip SET %s = '%s' WHERE clip_id = '%d' AND video_name = '%s'" % (key, value, clip_id, video_name))
    conn.commit()

def query_clip(conn, clip_id, video_name):
    c = conn.cursor()
    c.execute("SELECT * FROM clip WHERE clip_id = '%d' AND video_name = '%s'" % (clip_id, video_name))
    result = c.fetchall()
    return result

def query_background(conn, video_name, background_id=None, clip_id=None):
    c = conn.cursor()
    if background_id == None and clip_id == None:
        raise ValueError("Neither background_id nor clip_id is given")
    elif background_id != None and clip_id != None:
        c.execute("SELECT * FROM background WHERE background_id = '%d' AND clip_id = '%d' AND video_name = '%s'" % (background_id, clip_id, video_name))
    elif background_id != None and clip_id == None:
        c.execute("SELECT * FROM background WHERE background_id = '%d' and video_name = '%s'" % (background_id, video_name))
    elif background_id == None and clip_id != None:
        c.execute("SELECT * FROM background WHERE clip_id = '%d' and video_name = '%s'" % (clip_id, video_name))
    result = c.fetchall()
    return result

def query_label(conn, label, video_name):
    c = conn.cursor()
    c.execute("SELECT * FROM label WHERE label = '%s' AND video_name = '%s'" % (label, video_name))
    result = c.fetchall()
    return result

def query_everything(conn, video_name):
    c = conn.cursor()
    c.execute("SELECT * FROM label WHERE video_name = '%s'" % video_name)
    result = c.fetchall()
    return result

def cache(conn, video_name, clip_condition, rows=None, hwang=False):
    clip_ids = clip_condition.query(conn, video_name)
    for id in clip_ids:
        clip = query_clip(conn, id, video_name)
        videoref = clip[0][8]

        if not is_cache_file(videoref):
            cacheref = videoref_2_cache(videoref)

            #print('Recorded Length',clip[0][2],clip[0][3])
            vstream = VideoStream(videoref, rows=rows, hwang=hwang)
            length, height, width, channels = persist(vstream, cacheref)

            #if (clip[0][3]-clip[0][2]) != length:
            #    print('Mismatch',clip[0][2],clip[0][3], cacheref, length)

            update = "UPDATE clip SET video_ref = '%s', height=%d, width=%d WHERE clip_id = '%d' AND video_name = '%s'" % (cacheref,height, width,id, video_name)
            c = conn.cursor()
            c.execute(update)

def quality(conn, video_name, clip_condition, qscale, rscale, inplace):
    clip_ids = clip_condition.query(conn, video_name)
    for id in clip_ids:
        clip = query_clip(conn, id, video_name)
        videoref = clip[0][8]

        new_file = set_quality(videoref,videoref+"-"+str(rscale)+".avi",qscale, rscale)


        if inplace:
            os.remove(videoref)

        update = "UPDATE clip SET video_ref = '%s' WHERE clip_id = '%d' AND video_name = '%s'" % (new_file, id, video_name)
        c = conn.cursor()
        c.execute(update)

def uncache(conn, video_name, clip_condition):
    clip_ids = clip_condition.query(conn, video_name)
    for id in clip_ids:
        clip = query_clip(conn, id, video_name)
        cacheref = clip[0][8]
        
        if is_cache_file(cacheref):
            videoref = cache_2_videoref(cacheref)

            delete_cache(cacheref)

            update = "UPDATE clip SET video_ref = '%s' WHERE clip_id = '%d' AND video_name = '%s'" % (videoref,id, video_name)
            c = conn.cursor()
            c.execute(update)

            #print(update)


def query(conn, video_name, clip_condition, rows=None, hwang=False):
    """
    Args:
        conn (SQLite conn object) - please pass self.conn directly
        video_name (string) - identifier of the entire video (not clip)
        clip_condition (Condition object) - conditions of the query. e.g. Condition(filter='car')

    Returns:
        video_refs - list of VideoStream objects that you can iterate through
    """
    clip_ids = clip_condition.query(conn, video_name)

    video_refs = []
    boundaries = []
    for id in clip_ids:
        clip = query_clip(conn, id, video_name)

        clip_ref = clip[0][8]
        origin = np.array((clip[0][4],clip[0][5]))
        start_time, end_time = clip[0][2], clip[0][3]
        height, width = clip[0][6], clip[0][7]

        yield _create_vstream(clip_ref, start_time, end_time, \
                              height, width, origin, rows=rows, hwang=hwang)

        # NOTE: Old code that does not scale
        # vstream = _create_vstream(clip_ref, start_time, end_time, \
        #                           height, width, origin, rows=rows, hwang=hwang)
        # #print(clip[0][2], clip[0][3], clip[0])
        # video_refs.append(((start_time, end_time),vstream))

    # video_refs.sort(key=lambda tup: tup[0][0]) #sort by clip start
    #
    # if _is_contiguous(video_refs):
    #     return _chain_contiguous(video_refs)
    # else:
    #     return [v for _, v in video_refs]

def _create_vstream(ref, start_time, end_time, \
                    height, width, origin, rows=None, hwang=False):

    if not is_cache_file(ref):
        #print(ref)
        return VideoStream(ref,origin=origin, offset=start_time, rows=rows, hwang=hwang)
    else:
        return RawVideoStream(ref, shape=(end_time-start_time,height,width,3), origin=origin, offset=start_time)


def _is_contiguous(videos, thresh=5):
    if len(videos) == 0:
        return False

    prev = None
    for bounds, _ in videos:

        if prev is None or abs(prev - bounds[0]) < thresh:
            prev = bounds[1]
        else:
            return False

    return True

def _chain_contiguous(videos):
    vrefs = [v for _, v in videos]
    srcs = [v.src for _, v in videos]
    return [IteratorVideoStream(itertools.chain(*vrefs), srcs)]





