# TODO: VideoStream types: Hwang, OpenCV, Iterator
# TODO: Add error if next is called before initialize
import json
import cv2
from timeit import default_timer as timer
import numpy as np

from deeplens.struct import *


class DataStream():
    def __init__(self, data, name):
        self.name = name

    def __iter__(self):
        raise NotImplemented("__iter__ implemented")
    
    def get(self):
        raise NotImplemented("__iter__ implemented")
    
    def __next__(self):
        raise NotImplemented("__next__ not implemented")

    def init_mat(self):
        raise NotImplemented("init_mat not implemented")

    def append(self, data, prev):
        raise NotImplemented("append not implemented")

    def materialize(self, data):
        raise NotImplemented("materialize not implemented")

class JSONListDataStream(DataStream):
    def __init__(self, data, name):
        super.__init__(data, name)
        self.data = []
        if type(data) == str:
            files = [data]
        else:
            files = data
        for file in files:
            with open(file, 'r') as f:
                self.data.append(iter(json.load(f)))

    def __init__(self):
        self.index = 0

    def __next__(self):
        self.index += 1
        if self.index >= len(self.data):
            raise StopIteration("Iterator is closed")
        return self
    
    def get(self):
        return self.data[self.index - 1]
    
    def init_mat(self):
        return []

    def append(self, data, prev):
        return prev.append(data)

    def materialize(self, data, fp = None):
        if not file_name:
            return json.dumps(data)
        else:
            return json.dump(data, fp)

class ConstantDataStream(DataStream):
    def __init__(self, data, name):
        super.__init__(name)
        self.data = data

    def __next__(self):
        return self
    
    def get(self):
        return self.constants

    def init_mat(self):
        return None
    
    def append(self, data, prev):
        return data
    
    def materialize(self, data):
        return data

class VideoStream(DataStream):
    
    def __init__(self, src, name, limit=-1, origin = np.array((0,0)), offset = 0, start_time = 0):
        super.__init__(name)
        self.src = src
        self.limit = limit
        self.origin = origin
        self.offset = offset
        self.start_time = start_time
    
    def init_mat(self, file_name, encoding, frame_rate):
        super.init_mat()
    
    def append(self, data, prev):
        super.append(data, prev)
    
    def materialize(self, data):
        return True


class CVVideoStream(VideoStream):
    def __init__(self, src, name, limit = -1, origin = np.array((0,0)), offset = 0):
        super.__init__(name, src, limit, origin, offset)
        self.propIds = None
        self.frame = None

    def __iter__(self):
        if self.cap == None:
            # iterate the same videostream again after the previous run has finished
            self.frame_count = self.offset
            self.cap = cv2.VideoCapture(self.src)
        
        if self.propIds:
            for propId in self.propIds:
                self.cap.set(propId, self.propIds[propId])

        if not self.cap.isOpened():
            raise CorruptedOrMissingVideo(str(self.src) + " is corrupted or missing.")
        
        self.width = int(self.cap.get(3))   # float
        self.height = int(self.cap.get(4)) # float
        return self

    def next(self):
        if self.cap.isOpened() and \
            (self.limit < 0 or self.frame_count < self.limit):
            time_start = timer()
            ret, frame = self.cap.read()
            self.time_elapsed += timer() - time_start

            if ret:
                self.frame_count += 1
                self.frame = {'data': frame, \
                        'frame': (self.frame_count - 1),\
                        'origin': self.origin,
                        'width': self.width,
                        'height': self.hei}
            else:
                self.frame = None
        else:
            # self.cap.release()  # commented out due to CorruptedOrMissingVideo error
            self.cap = None
            self.frame = None
        if self.frame == None:
            raise StopIteration("Iterator is closed")
        else:
            return self
        
    def get(self):
        return self.frame
    
    def get_cap_info(self, propId):
        """ If we currently have a VideoCapture op
        """
        if self.cap:
            return self.cap.get(propId)
        else:
            return None

    def __call__(self, propIds = None):
        """ Sets the propId argument so that we can
        take advantage of video manipulation already
        supported by VideoCapture (cv2)
        Arguments:
            propIds: {'ID': property}
        """
        self.propIds = propIds

    def init_mat(self, file_name, encoding, frame_rate):
        fourcc = cv2.VideoWriter_fourcc(*encoding)
        cv2.VideoWriter(file_name,
                        fourcc,
                        frame_rate,
                        (self.width, self.height),
                        True)
        
    
    def append(self, data, prev):
        prev.write(data)
        return prev

class RawVideoStream(VideoStream):
    def __init__(self, src, name, limit=-1, origin=np.array((0,0)), offset = 0):
        super.__init__(src, name, limit, origin, offset)
        self.curr_frame = None
        self.next_frame = None
    
    def __iter__(self):
        self.origin = origin
        try:
            self.frame_iter = iter(self.src)
        except:
            raise CorruptedOrMissingVideo(str(self.src) + " is corrupted or missing.")
        try:
            frame = next(self.frame_iter)
            # set sizes after the video is opened
            self.width = int(self.next_frame.shape[0])  # float
            self.height = int(self.next_frame.shape[1])  # float
            self.frame_count = 0 
            self.next_frame = frame
        except StopIteration:
            self.next_frame = None
        return self
    
    def __next__(self):
        if self.next_frame is None:
            raise StopIteration("Iterator is closed")

        if (self.limit < 0 or self.frame_count <= self.limit - 1):
            self.curr_frame = self.next_frame
            frame = next(self.frame_iter)
            self.frame_count += 1
            self.next_frame = frame
        else:
            raise StopIteration("Iterator is closed")
    
    def get(self):
        return self.curr_frame

# TODO: Add implementation with hwang backend
class HwangVideoStream(VideoStream):
    def __init__(self, limit=-1):
        super.__init__(limit)

    def initialize(self, src, origin=np.array((0,0)), offset = 0):
        raise NotImplemented("initialize not implemented")