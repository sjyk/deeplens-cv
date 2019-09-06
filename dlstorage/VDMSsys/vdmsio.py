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
              vstream, \
              encoding, \
              header):
    
    tags = []
    
    for frame in vstream:
        header.update(frame)
        tags.append(frame['tags'])
    
    db = vdms.vdms()
    db.connect('localhost')
    fd = open(fname)
    blob = fd.read()
    all_queries = []
    addVideo = {}
    addVideo["container"] = "mp4"
    if encoding == H264:
        addVideo["codec"] = "h264"
    else:
        addVideo["codec"] = "xvid"
    
    addVideo["properties"] = header.getHeader()
    query = {}
    query["AddVideo"] = addVideo
    all_queries.append(query)
    response, res_arr = db.query(all_queries, [[blob]])
    print(response)
    db.disconnect()


def add_video_clips(fname, \
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
    
    numFrames = fps * size
    counter = 0
    clipCnt = 0
    props = {}
    for frame in vstream:
        header.update(frame)
        tags.append(frame['tags'])
        
        if counter == numFrames:
            props[clipCnt] = header.getHeader()
            header.reset()
            counter = 0
            clipCnt += 1
    
    db = vdms.vdms()
    db.connect('localhost')
    fd = open(fname)
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
    
    addVideo["properties"] = props
    
    query = {}
    query["AddVideoBL"] = addVideo
    all_queries.append(query)
    response, res_arr = db.query(all_queries, [[blob]])
    print(response)
    db.disconnect()
    

def find_video(vname, \
               condition, \
               size):
    db = vdms.vdms()
    db.connect("localhost")
    
    all_queries = []
    findVideo = {}
    constrs = {}
    constrs["name"] = ["==", name]
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