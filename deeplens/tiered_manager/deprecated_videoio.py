"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

deprecated_videoio.py contains videoio functions that are no longer used in
tiered_manager.py 
"""
from deeplens.full_manager.tiered_file import *
from deeplens.constants import *
from deeplens.struct import *
from deeplens.header import ObjectHeader
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

def write_video(vstream, \
                output, \
                encoding, \
                header,
                output_extern = None, \
                scratch= DEFAULT_TEMP, \
                frame_rate=DEFAULT_FRAME_RATE, \
                header_cmp=RAW):
    """write_video takes a stream of video and writes
    it to disk. It includes the specified header 
    information as a part of the video file.

    Args:
        vstream - a videostream or videotransform
        output - output file
        header - a header object that constructs the right
        header information
        scratch - temporary space to use
        frame_rate - the frame_rate of the video
        header_cmp - compression if any on the header
        output_extern - if the file goes to external 
        storage, specify directory
    """

    # Define the codec and create VideoWriter object
    fourcc = cv2.VideoWriter_fourcc(*encoding)
    start = True

    #tmp file for the video

    r_name = get_rnd_strng()
    if output_extern == None:
        seg_name = os.path.join(scratch, r_name)
    else:
        output_extern = output_extern +  '0'
        if not os.path.exists(output_extern):
            os.mkdir(output_extern)
        seg_name = os.path.join(output_extern, r_name)
    
    file_name = add_ext(seg_name, AVI)
    global_time_header = ObjectHeader(store_bounding_boxes=False)
    
    for frame in vstream:

        if start:

            out = cv2.VideoWriter(file_name,
                                  fourcc, 
                                  frame_rate, 
                                  (vstream.width, vstream.height),
                                  True)
            start = False

        header.update(frame)

        out.write(frame['data'])
        global_time_header.update(frame)

    if output_extern:
        ref_name = os.path.join(scratch, r_name)
        ref_file = add_ext(ref_name, '.txt')
        write_ref_file(ref_file, output_extern)
        file_name = ref_file #ref_file becomes the video

        ext = '.ref'
    else:
        ext = '.seq'
    seg_start_file = write_block(global_time_header.getHeader(), \
                                    None ,\
                                    add_ext(output, '.start'))
    
    header = header.getHeader()
    header['seq'] = 0
    return [seg_start_file, \
            build_fmt_file(header, \
                           file_name, \
                           scratch, \
                           add_ext(output, ext, 0), 
                           header_cmp, \
                           RAW,\
                           r_name)]


def write_video_clips(vstream, \
                        output, \
                        encoding, \
                        header,
                        clip_size,
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
        header - a header object that constructs the right
        header information
        clip_size - how many frames in each clip
        scratch - temporary space to use
        frame_rate - the frame_rate of the video
        header_cmp - compression if any on the header
        output_extern - if the file goes to external 
        storage, specify directory
    """

    # Define the codec and create VideoWriter object
    counter = 0
    seq = 0

    output_files = []

    global_time_header = ObjectHeader(store_bounding_boxes=False)
    #clip_size = min(global_time_header.end, clip_size)

    for frame in vstream:

        if counter == 0:
            #tmp file for the video
            r_name = get_rnd_strng()
            if output_extern:
                output_extern_seq = output_extern +  str(seq)
                if not os.path.exists(output_extern_seq):
                    os.mkdir(output_extern_seq)
                seg_name = os.path.join(output_extern_seq, r_name)
            else:
                seg_name = os.path.join(scratch, r_name)
            file_name = add_ext(seg_name, AVI, seq)
            fourcc = cv2.VideoWriter_fourcc(*encoding)

            out_vid = cv2.VideoWriter(file_name,
                                  fourcc, 
                                  frame_rate, 
                                  (vstream.width, vstream.height),
                                  True)

        out_vid.write(frame['data'])
        header.update(frame)
        global_time_header.update(frame)
        counter += 1

        if counter == clip_size:
            if output_extern:
                ref_name = output_extern +  str(seq)
                ref_file = add_ext(ref_name, '.txt')
                write_ref_file(ref_file, output_extern_seq)
                file_name = ref_file #ref_file becomes the video
                ext = '.ref'
            else:
                ext = '.seq'
            header_dict = header.getHeader()
            header_dict['seq'] = seq
            output_files.append(build_fmt_file(header_dict, \
                                                file_name, \
                                                scratch, \
                                                add_ext(output, ext, seq), \
                                                header_cmp, \
                                                RAW, 
                                                r_name))

            header.reset()
            out_vid.release()
            
            counter = 0
            seq += 1


    if counter != 0:
        if output_extern:
            ref_name = output_extern +  str(seq)
            if not os.path.exists(output_extern_seq):
                os.mkdir(output_extern_seq)
            #ref_name = os.path.join(scratch, r_name)
            
            ref_file = add_ext(ref_name, '.txt')
            write_ref_file(ref_file, output_extern_seq)
            file_name = ref_file #ref_file becomes the video
            ext = '.ref'
        else:
            ext = '.seq'
        header_dict = header.getHeader()
        header_dict['seq'] = seq
        output_files.append(build_fmt_file(header_dict, \
                                                file_name, \
                                                scratch, \
                                                add_ext(output, ext, seq), \
                                                header_cmp, \
                                                RAW, 
                                                r_name))

        header.reset()
        out_vid.release()
    

    output_files.append(write_block(global_time_header.getHeader(), \
                                    None ,\
                                    add_ext(output, '.start')))

    return output_files
