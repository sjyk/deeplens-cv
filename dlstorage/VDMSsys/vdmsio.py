"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

vdmsio.py uses the VDMS client to add and find videos. It contains
primitives to encode and decode archived and regular video formats.
"""
from dlstorage.core import *
from dlstorage.constants import *
from dlstorage.stream import *
from dlstorage.header import *
from dlstorage.xform import *

import vdms
import json
import cv2 #it looks like there's no choice but to use opencv because we need 
#to convert frames to seconds, or vice versa, and so we need the fps of 
#the original video

def add_video(fname, \
              vname, \
              vstream, \
              encoding, \
              header):
    
    tags = []
    totalFrames = 0
    for i,frame in enumerate(vstream):
        header.update(frame)
        tags.append(frame['tags'])
        totalFrames = i
    
    db = vdms.vdms()
    db.connect('localhost')
    fd = open(fname, 'rb')
    blob = fd.read()
    all_queries = []
    addVideo = {}
    addVideo["container"] = "mp4"
    """
    if encoding == H264:
        addVideo["codec"] = "h264"
    else:
        addVideo["codec"] = "xvid"
        Actually, I think you can only use h264 on VDMS
    """
    addVideo["codec"] = "h264"
    header_dat = header.getHeader()
    props = {}
    props[0] = header_dat
    props[0]["isFull"] = True
    props[0]["name"] = vname
    #addVideo["properties"] = props
    vprops = {}
    vprops["name"] = vname
    vprops["isFull"] = True
    addVideo["properties"] = vprops
    query = {}
    query["AddVideo"] = addVideo
    all_queries.append(query)
    response, res_arr = db.query(all_queries, [[blob]])
    print(response)
    db.disconnect()
    return totalFrames,props

def add_video_clips(fname, \
                    vname, \
                    vstream, \
                    encoding, \
                    header, \
                    size):

    tags = []

    video = cv2.VideoCapture(fname)
    #Find OpenCV version
    (major_ver, minor_ver, subminor_ver) = (cv2.__version__).split('.')
    if int(major_ver) < 3:
        fps = video.get(cv2.cv.CV_CAP_PROP_FPS)
    else:
        fps = video.get(cv2.CAP_PROP_FPS)
    
    numFrames = int(fps * size)
    counter = 0
    clipCnt = 0
    props = {}
    vprops = {}
    totalFrames = 0
    for i,frame in enumerate(vstream):
        header.update(frame)
        tags.append(frame['tags'])
        
        if counter == numFrames:
            props[clipCnt] = header.getHeader()
            props[clipCnt]["clipNo"] = clipCnt #add a clip number for easy
            #retrieval
            props[clipCnt]["numFrames"] = numFrames
            props[clipCnt]["isFull"] = False
            props[clipCnt]["name"] = vname
            header.reset()
            ithprops = {}
            ithprops["clipNo"] = clipCnt
            ithprops["name"] = vname
            ithprops["isFull"] = False
            vprops[clipCnt] = ithprops
            counter = 0
            clipCnt += 1
        totalFrames = i
    
    db = vdms.vdms()
    db.connect('localhost')
    fd = open(fname, 'rb')
    blob = fd.read()
    all_queries = []
    addVideo = {}
    addVideo["container"] = "mp4"
    if encoding == H264:
        addVideo["codec"] = "h264"
    else:
        addVideo["codec"] = "xvid"
    
    if size > 0:
        addVideo["clipSize"] = size
    
    addVideo["accessTime"] = 2
    
    addVideo["properties"] = vprops
    
    query = {}
    query["AddVideoBL"] = addVideo
    all_queries.append(query)
    response, res_arr = db.query(all_queries, [[blob]])
    print(response)
    db.disconnect()
    return totalFrames,props
    

def find_clip(vname, \
               condition, \
               size, \
               headers, \
               clip_no, \
               isFull):
    
    db = vdms.vdms()
    db.connect("localhost")
    
    all_queries = []
    findVideo = {}
    constrs = {}
    constrs["name"] = ["==", vname]
    if isFull:
        constrs["isFull"] = ["==", isFull]
    else:
        constrs["clipNo"] = ["==", clip_no]
    #add more filters based on the conditions
    
    findVideo["constraints"] = constrs
    findVideo["container"] = "mp4"
    findVideo["codec"] = "h264"
    
    query = {}
    query["FindVideo"] = findVideo
    
    all_queries.append(query)
    response, vid_arr = db.query(all_queries)
    print(response)
    db.disconnect()
    return vid_arr

def find_video(vname, \
               condition, \
               size, \
               headers, \
               totalFrames):
    
    clips = clip_boundaries(0, totalFrames-1, size)
    boundaries = []
    streams = []
    relevant_clips = set()
    #vid_arr is an array of video blobs, which we can't use in this case.
    #Therefore, we have to write them to disk first and then materialize them
    #using pre-stored header info and 
        
    for i in range(len(headers)):
        header_data = headers[i]
        isFull = header_data["isFull"]
        ithblob = find_clip(vname, condition, size, headers, i, isFull)
        #write the blob to file
        ithname = vname + str(i) + "tmp.mp4"
        ithf = open(ithname, 'wb')
        ithf.write(ithblob)
        ithf.close()
        if condition(header_data):
            pstart, pend = find_clip_boundaries((header_data['start'], \
                                                 header_data['end']), \
                                                 clips)
    
            relevant_clips.update(range(pstart, pend+1))
            boundaries.append((header_data['start'],header_data['end']))
        
        streams.append(VideoStream(ithname))
    
    relevant_clips = sorted(list(relevant_clips))
    
    return [materialize_clip(clips[i], boundaries, streams) for i in relevant_clips]