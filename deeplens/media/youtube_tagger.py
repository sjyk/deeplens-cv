import cv2
import numpy as np
import csv
from deeplens.struct import Box, Operator
from deeplens.struct import VideoStream
import os

"""
This file contains the tagger object,
and the tagging function used to tag youtube videos
with their provided bounding box
information
"""

"""
This is the class that holds all the relevant info for a video frame,
including bounding box information
"""
class FrameInfo(object):
    
    def __init__(self):
        self.youtubeID = ''
        self.second_no = -1
        self.fps = -1
        self.frame_no = -1
        self.obj_type = ''
        self.xmin = -1.0
        self.xmax = -1.0
        self.ymin = -1.0
        self.ymax = -1.0
    
    def __init__(self, youtubeID, second_no, fps, frame_no, obj_type, xmin, xmax, ymin, ymax):
        self.youtubeID = youtubeID
        self.second_no = second_no
        self.fps = fps
        self.frame_no = frame_no
        self.obj_type = obj_type
        self.xmin = float(xmin)
        self.xmax = float(xmax)
        self.ymin = float(ymin)
        self.ymax = float(ymax)
    
    def getLabel(self):
        return self.obj_type
    
    def getBox(self):
        return Box(self.xmin, self.ymin, self.xmax, self.ymax)

class YoutubeTagger(Operator):
    def __init__(self, video_url, labelsPath):
        #super(YoutubeTagger, self).__init__()
        self.video_url = video_url
        #I guess I have to manually generate a video stream here...
        self.video_stream = VideoStream(self.video_url)
        self.video_stream = iter(self.video_stream)
        self.width = self.video_stream.width
        self.height = self.video_stream.height
        self.labelsPath = labelsPath
        self.csvDict = self.getAllYTTags()

    # subscripting binds a transformation to the current stream
    def apply(self, vstream):
        self.video_stream = vstream
        return self

    def __getitem__(self, xform):
        """Applies a transformation to the video stream
        """
        return xform.apply(self)
    
    def getAllYTTags(self):
        fd = open(self.labelsPath, 'r')
        labelReader = csv.reader(fd)
        outputDict = dict()
        for i,row in enumerate(labelReader):
            if (i== 0): #skip the header--we'll assume we know what it is
                continue
            else:
                frInfo = self.toFrameInfo(row)
                youtubeID = frInfo.youtubeID
                if youtubeID in outputDict:
                    lstOfFrameInfos = outputDict[youtubeID]
                    lstOfFrameInfos.append(frInfo)
                    outputDict[youtubeID] = lstOfFrameInfos
                else:
                    nlst = [frInfo]
                    outputDict[youtubeID] = nlst
        fd.close()
        #sort by frame_no
        for key in outputDict:
            lstOfFrameInfos = outputDict[key]
            lstOfFrameInfos = sorted(lstOfFrameInfos, key=lambda x: x.frame_no)
            outputDict[key] = lstOfFrameInfos
        return outputDict
    
    def toFrameInfo(self, row):
        # on my laptop, the to_csv function in pandas generates columns in alphabetical order
        youtubeID = row[0]
        sec_no = float(row[1])
        fps = int(row[2])
        frame_no = int(row[3])
        obj_type = row[4]
        xmin = float(row[5])
        xmin = xmin * self.width
        xmax = float(row[6])
        xmax = xmax * self.width
        ymin = float(row[7])
        ymin = ymin * self.height
        ymax = float(row[8])
        ymax = ymax * self.height
        return FrameInfo(youtubeID, sec_no, fps, frame_no, obj_type, xmin, xmax, ymin, ymax)
        # youtubeID = row[8]
        # sec_no = float(row[3])
        # fps = int(row[1])
        # frame_no = int(row[2])
        # obj_type = row[0]
        # xmin = float(row[5])
        # xmin = xmin * self.width
        # xmax = float(row[4])
        # xmax = xmax * self.width
        # ymin = float(row[7])
        # ymin = ymin * self.height
        # ymax = float(row[6])
        # ymax = ymax * self.height
        # return FrameInfo(youtubeID, sec_no, fps, frame_no, obj_type, xmin, xmax, ymin, ymax)

    def __iter__(self):
        self.input_iter = iter(self.video_stream)
        #self.super_iter()
        return self
    
    def parseID(self):
        head_tail = os.path.split(self.video_url)
        fname = head_tail[1]
        if len(fname) < 5:
            raise Exception("Format error: expecting a file name with an extension")
        return fname[:-4]
        
    
    def _get_tag(self, frame_count):
        res_tag = {'objects': []}
        videoID = self.parseID()
        labelLst = self.csvDict[videoID]
        #we can switch to binary search later...
        for l in labelLst:
            if frame_count <= l.frame_no:
                res_tag = {'objects': [{'label' : l.getLabel(), 'bb' : l.getBox()}]}
                return res_tag
        return res_tag
    
    def __next__(self):
        frame_count = self.video_stream.__next__()['frame']
        return self._get_tag(frame_count)
                
                
                

# class YoutubeTagger(Operator):
#     def __init__(self, video_url, batch_size, labelsPath):
#         super(YoutubeTagger, self).__init__()
#         self.video_url = video_url
#         self.batch_size = batch_size
#         self.row_count = 0
#         self.next_count = 0
#         #get the open file descriptor for reading the label/bb info
#         self.fd = open(labelsPath, 'r')
#         self.labelReader = csv.reader(self.fd)
#         #throw away the header
#         header = self.labelReader.next()
#         row1 = self.labelReader.next()
#         info1 = self.toFrameInfo(row1)
#         self.cur_frame = 0 #video frame counter
#         self.cur_info = info1
#         self.prev_info = info1
    
#     def __iter__(self):
#         self.input_iter = iter(self.video_stream)
#         self.super_iter()
#         return self
    
#     def _get_tags(self):
#         if self.next_count == 0 or self.next_count >= self.batch_size:
#             self.next_count = 0
#             # we assume it iterates the entire batch size and save the results
#             self.tags = []
#             try:
#                 cur_tags = self.youtubeTag()
#             except StopIteration:
#                 raise StopIteration("Iterator is closed")
#             self.tags += cur_tags
        
#         self.next_count += 1
#         return self.tags
                
#     def __next__(self):
#         return {'objects' : self._get_tags()}
    
#     def toFrameInfo(self, row):
#         youtubeID = row[0]
#         sec_no = row[1]
#         fps = row[2]
#         frame_no = row[3]
#         obj_type = row[4]
#         xmin = float(row[5])
#         xmax = float(row[6])
#         ymin = float(row[7])
#         ymax = float(row[8])
#         return FrameInfo(youtubeID, sec_no, fps, frame_no, obj_type, xmin, xmax, ymax, ymin)
        
    
#     def youtubeTag(self):
#         res_tags = []
#         count = 0
#         for frame in self.input_iter:
#             vid_name = self.cur_info.youtubeID + '.mp4'
#             if not vid_name in self.video_url:
#                 tmp_info = FrameInfo()
#                 while (not vid_name in self.video_url):
#                     try:
#                         row = self.labelReader.next()
#                         tmp_info = self.toFrameInfo(row)
#                         vid_name = self.tmp_info.youtubeID + '.mp4'
#                     except StopIteration:
#                         #we must have all the tags we need...
#                         return res_tags
                    
#                 self.cur_info = tmp_info
#                 self.prev_info = copy.deepcopy(tmp_info)
#             if self.cur_frame == self.cur_info.frame_no:
#                 new_tag = [{'label' : self.cur_info.getLabel(), 'bb' : self.cur_info.getBox()}]
#                 res_tags.append(new_tag)
#                 self.prev_info = copy.deepcopy(self.cur_info)
#                 self.cur_info = self.toFrameInfo(self.labelReader.next())
#             elif self.cur_frame < self.cur_info.frame_no:
#                 new_tag = [{'label' : self.prev_info.getLabel(), 'bb' : self.prev_info.getBox()}]
#                 res_tags.append(new_tag)
#             elif self.cur_frame > self.cur_info.frame_no:
#                 #catch the frame info up to the right frame number
#                 tmp_info = self.cur_info
#                 p_info = self.cur_info
#                 r_cnt = 0
#                 for row in self.labelReader:
#                     p_info = copy.deepcopy(tmp_info)
#                     tmp_info = self.toFrameInfo(row)
#                     vid_name = self.tmp_info.youtubeID + '.mp4'
#                     if self.cur_frame <= tmp_info.frame_no and vid_name in self.video_url:
#                         #the iterator over labelReader is now at the right spot, so stop
#                         self.cur_info = tmp_info
#                         self.prev_info = p_info
#                         break
#                     r_cnt += 1
#                 if r_cnt == 0:
#                     #just copy the latest frame info for the rest of the frames
#                     new_tag = [{'label' : self.cur_info.getLabel(), 'bb' : self.cur_info.getBox()}]
#                     res_tags.append(new_tag)
                        
#             self.cur_frame += 1 #again, there might be an off-by-one error here
#             count+= 1
#             if count >= self.batch_size:
#                 break
        
#         if count == 0:
#             raise StopIteration("Iterator is closed")
        
#         return res_tags




# def youtubeTag(path, output):
#     fullpath = path + output
#     fh = open(fullpath, 'r')
#     csvreader = csv.reader(fh)
#     outputDict = dict()
#     for i,row in enumerate(csvreader):
#         if (i== 0): #skip the header--we'll assume we know what it is
#             continue
#         else:
#             youtubeID = row[0]
#             sec_no = row[1]
#             fps = row[2]
#             frame_no = row[3]
#             obj_type = row[4]
#             xmin = row[5]
#             xmax = row[6]
#             ymin = row[7]
#             ymax = row[8]
            
#             frInfo = FrameInfo(sec_no, fps, frame_no, obj_type, xmin, xmax, ymin, ymax)
#             if youtubeID in outputDict:
#                 lstOfFrameInfos = outputDict[youtubeID]
#                 lstOfFrameInfos.append(frInfo)
#                 outputDict[youtubeID] = lstOfFrameInfos
#             else:
#                 nlst = [frInfo]
#                 outputDict[youtubeID] = nlst
#     fh.close()
    
#     for yid in outputDict:
#         outputDict[yid]
            
            
