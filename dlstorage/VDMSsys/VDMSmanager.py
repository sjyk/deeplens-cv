"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

VDMSmanager.py defines the basic storage api in deeplens for storing in
VDMS, rather than the filesystem.
"""
from dlstorage.core import *
from dlstorage.constants import *
from dlstorage.stream import *
from dlstorage.filesystem.videoio import *
from dlstorage.header import *
from dlstorage.xform import *
from dlstorage.VDMSsys.vdmsio import *

import os
import math
import subprocess
import datetime
import time
import vdms
import sys
import requests
import cv2

class VDMSStorageManager(StorageManager):
    #NOTE: Here, size refers to the duration of the clip, 
    #and NOT the number of frames!
    #Another NOTE: We assume that if a VDMSStorageManager instance is used to 
    #put clips in VDMS, then the same instance must be used to get() the clips
    #Yet another NOTE: we are also assuming a VDMSStorageManager instance has
    #to be used to put() and get() exactly one file, for now
    DEFAULT_ARGS = {'encoding': H264, 'size': -1, 'limit': -1, 'sample': 1.0}
    
    def __init__(self, content_tagger):
        self.content_tagger = content_tagger
        self.clip_headers = []
        self.totalFrames = -1
        self.videos = set()
    
    def put(self, filename, target, args=DEFAULT_ARGS):
        """In this case, put() adds the file to VDMS, along with
        the header, which we might be able to send either as a long string
        of metadata, or as extra properties (which is still a lot of metadata)
        Note: we are going to suffer the performance penalty for acquiring
        header information in a frame-by-frame fashion
        Also Note: target is a dummy variable in this case, for the purposes
        of running the same benchmark
        """
        self.videos.add(target)
        v = VideoStream(filename, args['limit'])
        v = v[Sample(args['sample'])]
        v = v[self.content_tagger]
        #find the size of the inputted file: if it's greater than 32MB, VDMS is going to have issues,
        #so we'll have to split the file into clips anyway
        if 'http://' in filename or 'https://' in filename:
            response = requests.head(filename)
            fsize = float(response.headers['Content-Length']) / 1000000.0
        else:
            fsize = os.path.getsize(filename) / 1000000.0
        
        #if the file comes from a url, we need to write the video to disk first
        if 'http://' in filename or 'https://' in filename:
            fourcc = cv2.VideoWriter_fourcc(*encoding)
            urllst = filename.split('/')
            file_name = urllst[-1]
            video = cv2.VideoCapture(filename)
            #Find OpenCV version
            (major_ver, minor_ver, subminor_ver) = (cv2.__version__).split('.')
            if int(major_ver) < 3:
                frame_rate = video.get(cv2.cv.CV_CAP_PROP_FPS)
            else:
                frame_rate = video.get(cv2.CAP_PROP_FPS)
            
            out = cv2.VideoWriter(file_name,
                                  fourcc, 
                                  frame_rate, 
                                  (v.width, v.height),
                                  True)
            
            for frame in v:
                out.write(frame['data'])
            out.release()
  
        if args['size'] == -1 and fsize <= 32.0:
            tf, headers = add_video(file_name, target, v, args['encoding'], ObjectHeader())
            self.clip_headers = headers
            self.totalFrames = tf
        elif fsize > 32.0:
            #compute the number of clips we would need for each to be 32 MB
            numClips = int(math.ceil(fsize / 32.0))
            #find duration of video file to compute clip duration
            result = subprocess.Popen(['ffprobe', filename], \
                                      stdout = subprocess.PIPE, stderr = subprocess.STDOUT)
            dur = [x for x in result.stdout.read().decode('ascii').splitlines(True) if "Duration" in x]
            #convert duration string to seconds
            inf = dur[0].split(',')
            dInfo = "".join(inf[0].split())
            acdur = dInfo[len("Duration:"):]
            x = time.strptime(acdur.split('.')[0], '%H:%M:%S')
            fdur = datetime.timedelta(hours=x.tm_hour,minutes=x.tm_min,seconds=x.tm_sec).total_seconds()
            idur = int(fdur)
            clip_size = int(idur / numClips)
            #load the clips into VDMS
            tf, headers = add_video_clips(file_name, target, v, args['encoding'], ObjectHeader(), clip_size)
            self.clip_headers = headers
            self.totalFrames = tf
        else:
            tf, headers = add_video_clips(file_name, target, v, args['encoding'], ObjectHeader(), args['size'])
            self.clip_headers = headers
            self.totalFrames = tf
    
    def get(self, name, condition, clip_size, threads=1):
        """
        get() retrieves all the clips with the given name that satisfy the given condition.
        NOTE: clip_size is in FRAMES, not duration
        """
        return find_video(name, condition, clip_size, self.clip_headers, self.totalFrames, threads)
    
    def delete(self, name):
        """
        Note that VDMS currently does not support delete operations, making
        it impossible to delete the clips on the physical level. Therefore,
        we simply 'forget' the clips on the deeplens level for now
        """
        self.clip_headers = [] #assuming all header info is for one file
        self.videos.remove(name)
    
    def list(self):
        return list(self.videos)
    
    def size(self, name):
        """
        retrieve all clips with given name,
        and add together their sizes
        """
        db = vdms.vdms()
        db.connect("localhost")
        
        all_queries = []
        findVideo = {}
        constrs = {}
        constrs["name"] = ["==", name]
        #Assumption: The header information has not been overwritten by
        #header information for a different file
        if len(self.clip_headers) > 1:
            constrs["isFull"] = ["==", False]
        else:
            constrs["isFull"] = ["==", True]
        findVideo["constraints"] = constrs
        findVideo["container"] = "mp4"
        findVideo["codec"] = "h264"
        
        query = {}
        query["FindVideo"] = findVideo
        all_queries.append(query)
        response, vid_arr = db.query(all_queries)
        #print(response)
        db.disconnect()
        
        if len(vid_arr) < 1:
            return -1
        
        size = 0
        for v in vid_arr:
            size += sys.getsizeof(v)
        
        return size
