"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered_videoio.py uses opencv (cv2) to read and write files to disk. It contains
primitives to encode and decode archived and regular video formats for a tiered
storage system.
"""

from deeplens.tiered_manager.tiered_file import *
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

# TODO: fix headers and figure out what we're storing as crop group
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

    # Define the codec and create VideoWriter object
    start = True
    seq = 0

    output_files = []

    global_time_header = {}
    header = {}
    update_global_header = ObjectHeader(global_time_header,\
                                        store_bounding_boxes=False, offset=header_info['offset'])
    
    out_vids = []
    r_names = []
    file_names = []
    crops = []
    crop_positions = []
    for frame in vstream:
        if start or frame['split']:
            # write previous cropped clip segments to storage
            base_seq = seq
            if not start:
                for i in range(len(crops) + 1): 
                    if output_extern:
                        ref_name = os.path.join(scratch, r_name)
                        ref_file = add_ext(ref_name, '.txt')
                        write_ref_file(ref_file, file_names[i])
                        file_names[i] = ref_file #ref_file becomes the video
                        ext = '.ref'
                    else:
                        ext = '.seq'
                    header_dict = header.getHeader()
                    if i == 0 and len(crops) != 0:
                        header_dict['crop_group'] = base_seq + len(crops)
                    elif len(crops) != 0:
                        header_dict = crop_header(header_dict, crops[i - 1])
                        header_dict['crop_id'] = base_seq
                    if i != 0:
                        header_dict['crop_position'] = crop_positions[i - 1]
                    header_dict['seq'] = seq
                    output_files.append(build_fmt_file(header_dict, \
                                                        file_names[i], \
                                                        scratch, \
                                                        add_ext(output, ext, seq), \
                                                        header_cmp, \
                                                        RAW, 
                                                        r_names[i]))

                    out_vids[i].release()
                    seq += 1
                r_names = []
                file_names = []
                out_vids = []
                header.reset()
            crops = frame['crop']
            #tmp file for the video
            for i in range(len(crops) + 1):
                crop = crops[i - 1]
                if i != 0:
                    crop_positions.append({0: (crop[0], crop[1])}) # we store the top left corner
                r_name = get_rnd_strng()
                if output_extern:
                    output_extern_seq = output_extern +  str(seq + i)
                    if not os.path.exists(output_extern_seq):
                        os.mkdir(output_extern_seq)
                    seg_name = os.path.join(output_extern_seq, r_name)
                    file_names.append(output_extern_seq)
                else:
                    seg_name = os.path.join(scratch, r_name)
                file_name = add_ext(seg_name, AVI, seq + i)
                fourcc = cv2.VideoWriter_fourcc(*encoding)
                if not output_extern:
                    file_names.append(file_name)
                r_names.append(r_name)
                if i == 0:
                    width = vstream.width
                    height = vstream.height
                else:
                    width = abs(crops[i - 1][0] - crops[i - 1][2])
                    height = abs(crops[i - 1][1] - crops[i - 1][3])
                out_vid = cv2.VideoWriter(file_name,
                                        fourcc, 
                                        frame_rate, 
                                        (width, height),
                                        True)
                out_vids.append(out_vid)
            start = False

        update_crop = False
        # update cropped frames
        if len(frame['crop']) != 0:
            crops = frame['crop'] #note that even if we change the size/location of the crops, they remain in the same clip
            update_crop  = True
        i = 0
        if len(crops) == 0:
            out_vids[i].write(frame['data'])
            i +=1
        else:
            out_vids[i].write(reverse_crop(frame['data'], crops))
            i +=1

        for cr in crops:
            if update_crop:
                crop_positions[i][frame['frame']] = (cr[0], cr[1])
            fr = crop_box(frame['data'], cr)
            out_vids[i].write(fr)
            i +=1
        
        header.update(frame)
        global_time_header.update(frame)

    # write last segment
    base_seq = seq
    for i in range(len(crops) + 1): 
        if output_extern:
            ref_name = os.path.join(scratch, r_name)
            ref_file = add_ext(ref_name, '.txt')
            write_ref_file(ref_file, file_names[i])
            file_names[i] = ref_file #ref_file becomes the video
            ext = '.ref'
        else:
            ext = '.seq'
        header_dict = header.getHeader()
        if i == 0 and len(crops) != 0:
            header_dict['crop_group'] = base_seq + len(crops)
        elif len(crops) != 0:
            header_dict = crop_header(header_dict, crops[i - 1])
            header_dict['crop_id'] = base_seq
        header_dict['seq'] = seq
        output_files.append(build_fmt_file(header_dict, \
                                            file_names[i], \
                                            scratch, \
                                            add_ext(output, ext, seq), \
                                            header_cmp, \
                                            RAW, 
                                            r_names[i]))

        out_vids[i].release()
        seq += 1

    output_files.append(write_block(global_time_header.getHeader(), \
                                    None ,\
                                    add_ext(output, '.start')))

    return output_files

def _update_storage_header(file_path, header):
    StorageHeader(header).update()
    write_block(header, None, file_path)

#gets a file of a particular index and if there are external files
#consider parallelizing this better
def file_get(file):
    parsed = ncpy_unstack_block(file)
    
    if '.head' in parsed[0]:
        head, video = 0, 1
    else:
        head, video = 1, 0

    header = unstack_block(parsed[head], DEFAULT_TEMP, compression_hint=RAW)
    header_data = read_block(header[0])
    return header_data, parsed[video], is_ref_name(file), parsed[head]

def _all_files(output):
    rtn = []

    seq = 0
    while True:

        file = add_ext(output, '.seq', seq)

        if os.path.exists(file):
            rtn.append(file)
            seq += 1
            continue

        file = add_ext(output, '.ref', seq)
        if os.path.exists(file):
            rtn.append(file)
            seq += 1
            continue
        break
        

    return rtn

#delete a video
def delete_video_if_exists(output):

    start_file = add_ext(output, '.start')

    if not os.path.exists(start_file):
        return
    print(output)
    os.remove(start_file)
    seq = 0

    while True:
        f = add_ext(output, '.seq', seq)
        is_seq = path.exists(f)
        if is_seq:
            shutil.rmtree(f)
            seq += 1
            continue
        f = add_ext(output, '.ref', seq)
        is_ref = path.exists(f)
        if is_ref:
            parsed = ncpy_unstack_block(f)
            if '.head' in parsed[0]:
                video_ref = parsed[1]
            else:
                video_ref = parsed[0]
            seq_name = read_ref_file(video_ref)
            shutil.rmtree(seq_name)
            shutil.rmtree(f)
            seq += 1
            continue
        break



#counter using the start and end
def move_to_extern_if(output, condition, output_extern, threads=None):
    """move_to_extern_if takes a written archive file and writes to extern if 
    those video clips that satisfy a certain header condition.

    Args:
        output (string) - internal url
        condition (lambda) - a condition on the header content
        output_extern (string) - external url
    """
    if threads == None:
        # this isn't optimal because we could easily skip over groups
        pre_parsed = {file: file_get(file) for file in _all_files(output)}
    else:
        pre_parsed = threads.map(file_get, _all_files(output))

    rtn = None
    for (header, clip, is_extern, _) in pre_parsed.items():
        if condition(header):
            if not is_extern:
                clip_file = os.path.basename(clip)
                seq = header_data['seq']
                extern_dir = output_extern + str(seq)
                if not os.path.exists(extern_dir):
                    os.mkdir(extern_dir)
                vid_file = os.path.join(extern_dir, clip_file)
                os.rename(clip, vid_file)

                seq_dir = os.path.dirname(clip)
                ref_dir = seq_dir[:seq_dir.rfind('.')]           
                ref_dir = add_ext(ref_dir, '.ref')
                shutil.move(seq_dir, ref_dir)

                clip_string = clip_file[:clip_file.rfind('.')]
                clip_string = add_ext(clip_string, '.txt')
                ref_file = os.path.join(ref_dir, clip_string)
                write_ref_file(ref_file, extern_dir)
            rtn = output
    return rtn

def move_from_extern_if(output, condition, threads=None):
    """move_to_extern_if takes a written archive file and writes to extern if 
    those video clips that satisfy a certain header condition.

    Args:
        output (string) - directory
        condition (lambda) - a condition on the header content
        output_extern (string) - external directory
    """
    if threads == None:
        pre_parsed = {file_get(file) for file in _all_files(output)}
    else:
        pre_parsed = threads.map(file_get, _all_files(output))

    rtn = []
    for (header, ref_file, is_extern, _) in pre_parsed.items():
        if condition(header):
            if is_extern:
                extern_dir = read_ref_file(ref_file)
                
                if extern_dir.endswith('/'):
                    extern_dir = extern_dir[:-1]

                base_dir = os.path.join(Path(output).parent, os.path.basename(extern_dir))
                seq_dir = add_ext(base_dir, '.seq')
                ref_dir = add_ext(base_dir, '.ref')

                delete_ref_file(ref_file)
                shutil.move(ref_dir, seq_dir) 
                files = os.listdir(extern_dir)
                for f in files:
                    if f.endswith('.avi'):
                        f_full = os.path.join(extern_dir, f)
                        shutil.move(f_full, seq_dir) 

                os.rmdir(extern_dir)
            rtn = output
    return rtn

def check_extern_if(output, condition, threads=None):
    """move_to_extern_if takes a written archive file and writes to extern if 
    those video clips that satisfy a certain header condition.

    Args:
        output (string) - archive file
        condition (lambda) - a condition on the header content
        scratch (string) - a temporary file path
    """
    seq = 0
    if threads == None:
        pre_parsed = {file_get(file) for file in _all_files(output)}
    else:
        pre_parsed = threads.map(file_get, _all_files(output))

    rtn = []
    for (header, clip, is_extern, _) in pre_parsed.items():
        if condition(header):
            if is_extern:
                return True
    return False

#counter using the start and end
def read_if(output, condition, clip_size=5, scratch = DEFAULT_TEMP, threads=None):
    """read_if takes a written archive file and reads only
    those video clips that satisfy a certain header condition.

    Args:
        output (string) - archive file
        condition (lambda) - a condition on the header content
        scratch (string) - a temporary file path
    """    

    #read the meta data
    seg_start_data = read_block(add_ext(output, '.start'))
    clips = clip_boundaries(seg_start_data['start'],\
                            seg_start_data['end'],\
                            clip_size)

    boundaries = []
    streams = []
    relevant_clips = set()

    if threads == None:
        pre_parsed = [file_get(file) for file in _all_files(output)]
    else:
        pre_parsed = threads.map(file_get, _all_files(output))

    for header, clip, is_extern, header_file in pre_parsed:
        if condition(header):
            _update_storage_header(header_file, header)
            pstart, pend = find_clip_boundaries((header['start'], \
                                                 header['end']), \
                                               clips)
          
            for rel_clip in range(pstart, pend+1):
                    
                cH = cut_header(header, clips[rel_clip][0], clips[rel_clip][1])

                if condition(cH):
                    relevant_clips.add(rel_clip)

            boundaries.append((header['start'],header['end']))
        if is_extern:
            vid_dir = read_ref_file(clip)
            vid_clip = None
            for f in os.listdir(vid_dir):
                if f.endswith('.avi'):
                    vid_clip = os.path.join(vid_dir, f)
            streams.append([VideoStream(vid_clip), header['start']])
        else:
            streams.append([VideoStream(clip), header['start']])

    #sort the list
    relevant_clips = sorted(list(relevant_clips))
    return [materialize_clip(clips[i], boundaries, streams) for i in relevant_clips]

