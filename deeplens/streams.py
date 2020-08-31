# TODO: VideoStream types: Hwang, OpenCV, Iterator
# TODO: Add error if next is called before initialize
import json
import os
import tempfile
from shutil import copyfile
from subprocess import Popen
from timeit import default_timer as timer
import numpy as np
import cv2
import threading

from deeplens.utils.error import *


class DataStream():
    def __init__(self, name, stream_type):
        self.name = name
        self.type = stream_type
        self.iters = {}

    def add_iter(self, op_name):
        raise NotImplemented("__iter__ implemented")
    
    def next(self, op_name):
        raise NotImplemented("__iter__ implemented")
    
    @staticmethod
    def init_mat():
        raise NotImplemented("init_mat not implemented")

    @staticmethod
    def append(data, prev):
        raise NotImplemented("append not implemented")

    @staticmethod
    def materialize(self, data):
        raise NotImplemented("materialize not implemented")

class JSONListStream(DataStream):
    def __init__(self, data, name, stream_type, limit = -1, isList = False):
        super().__init__(name, stream_type)
        self.data = []
        self.limit = limit
        if data is not None:
            if isList:
                if type(data) == str:
                    files = [data]
                else:
                    files = data
                for file in files:
                    with open(file, 'r') as f:
                        self.data = self.data + json.load(f)
            else:
                if type(data) == str:
                    files = [data]
                else:
                    files = data
                for file in files:
                    with open(file, 'r') as f:
                        self.data = self.data.append(json.load(f))

    def add_iter(self, op_name):
        self.iters[op_name] = (iter(self.data), -1)
        return self

    def next(self, op_name):
        self.iters[op_name][1] += 1
        index = self.iters[op_name][1]
        #print(self.limit)
        if index >= len(self.data) or (index > self.limit and self.limit > 0):
            raise StopIteration("Iterator is closed")
        return next(self.iters[op_name][0])
    
    def serialize(self, fp = None):
        if not fp:
            return json.dumps(self.data)
        else:
            return json.dump(self.data, fp)
    
    def size(self):
        return len(self.data)

    @staticmethod
    def init_mat():
        return []

    @staticmethod
    def append(data, prev):
        return prev.data.append(data)

    @staticmethod
    def materialize(data, fp = None):
        if not fp:
            return json.dumps(data)
        else:
            return json.dump(data, fp)

class JSONDictStream(DataStream):
    def __init__(self, data, name, stream_type, limit = 0):
        super().__init__(name, stream_type)
        self.data = {}
        if data is not None:
            if type(data) == str:
                files = [data]
            else:
                files = data
            for file in files:
                with open(file, 'r') as f:
                    self.data = self.data.update(json.load(f))
        self.limit = limit

    def add_iter(self, op_name):
        self.iters[op_name] = -1
        return self

    def next(self, op_name):
        self.iters[op_name] += 1
        index = self.iters[op_name]
        if self.limit and index >= self.limit:
            raise StopIteration("Iterator is closed")
        if index in self.data:
            return self.data[index]
        else:
            return None

    def size(self):
        return self.limit
    
    def update_limit(self, limit):
        self.limit = limit

    def add(self, data, index):
        self.data[index] = data

    def update(self, stream):
        self.data.update(stream.data)

    def serialize(self, fp = None):
        if not fp:
            return json.dumps(self.data)
        else:
            return json.dump(self.data, fp)
    
    @staticmethod
    def init_mat():
        return {}

    @staticmethod
    def append(data, prev):
        prev.data[data[0]] = data[1]
        return prev

    @staticmethod
    def materialize(data, fp = None):
        if not fp:
            return json.dumps(data)
        else:
            return json.dump(data, fp)


class ConstantStream(DataStream):
    def __init__(self, data, stream_type, name):
        super().__init__(name, stream_type)
        self.data = data

    def add_iter(self, op_name):
        return self
    
    def next(self, op_name):
        return self.data
    
    @staticmethod
    def init_mat():
        return None
    
    @staticmethod
    def append(data, prev):
        return data
    
    @staticmethod
    def materialize(data):
        return data
    

class VideoStream(DataStream):
    def __init__(self, name, src, limit=-1, origin = np.array((0,0)), offset = 0, start_time = 0):
        super().__init__(name, 'video')
        self.src = src
        self.limit = limit
        self.origin = origin
        self.offset = offset
        self.start_time = start_time
    
    @staticmethod
    def init_mat(self, file_name, encoding, frame_rate):
        super.init_mat()
    
    @staticmethod
    def append(self, data, prev):
        super.append(data, prev)
    
    @staticmethod
    def materialize(self, data):
        return True


class CVVideoStream(VideoStream):
    def __init__(self, src, name, limit = -1, origin = np.array((0,0)), offset = 0):
        super().__init__(name, src, limit, origin, offset)
        import cv2
        self.cap = cv2.VideoCapture(self.src)
        self.width = int(self.cap.get(3))   # float
        self.height = int(self.cap.get(4)) # float
        if limit == -1:
            self.frame_num = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        else:
            self.frame_num = limit
        self.iters = {}

    def add_iter(self, op_name, propIds = None):
        self.iters[op_name] = (cv2.VideoCapture(self.src), self.offset)
        
        if propIds:
            for propId in propIds:
                self.iters[op_name][0].set(propId, self.propIds[propId])

        if not self.cap.isOpened():
            raise CorruptedOrMissingVideo(str(self.src) + " is corrupted or missing.")
        return self

    def next(self, op_name):
        cap = self.iters[op_name][0]
        frame_count = self.iters[op_name][1]
        if cap.isOpened() and (self.limit < 0 or frame_count < self.limit - 1):
            ret, frame = cap.read()
            if ret:
                frame_count += 1
                self.iters[op_name][1] += 1
            else:
                del self.iters[op_name]
                raise StopIteration("Iterator is closed")
        else:
            del self.iters[op_name]
            raise StopIteration("Iterator is closed")
        
        return frame
    
    def get_cap_info(self, propId):
        """ If we currently have a VideoCapture op
        """
        if self.cap:
            return self.cap.get(propId)
        else:
            return None

    @staticmethod
    def init_mat(file_name, encoding, width, height, frame_rate):
        fourcc = cv2.VideoWriter_fourcc(*encoding)
        writer = cv2.VideoWriter(file_name,
                        fourcc,
                        frame_rate,
                        (width, height),
                        True)
        return writer
        
    @staticmethod
    def append(data, prev):
        prev.write(data)
        return prev

class HwangVideoStream(VideoStream):
    def __init__(self, src, name, limit=-1, origin=np.array((0, 0))):
        super().__init__(src, limit, name, origin)
        import hwang
        self.frame_count = 0
        self.decoder = hwang.Decoder(self.src)
        self.width = self.decoder.video_index.frame_width()
        self.height = self.decoder.video_index.frame_height()
        if limit == -1:
            cap = cv2.VideoCapture(self.src)
            self.frame_num = int(cap.get(cv2.CAP_PROP_FRAME_COUNT)) 
        else:
            self.frame_num = limit
        self.frames = self.decoder.retrieve(list(range(self.frame_num))) # TODO(ted): make sure that this is getting every frame?
        self.iters = {}

    def add_iter(self, op_name):
        self.iters[op_name] = -1
        return self

    def next(self, op_name):
        self.iters[op_name] += 1
        
        if self.iters[op_name] >= len(self.frames):
            del self.iters[op_name]
            raise StopIteration("Iterator is closed")

        return self.frames[self.iters[op_name]]

    def all(self):
        return self.frames

    def init_mat(self):
        raise NotImplemented("init_mat not implemented")

    def append(self, data, prev):
        raise NotImplemented("append not implemented")

    def materialize(self, data):
        raise NotImplemented("materialize not implemented")

# no threading unlike open cv version -> can implement later
class HwangVideoStreams(VideoStream):
    def __init__(self, src, name, limit = -1, origin = np.array((0,0)), offset = 0, test_limit = -1):
        super().__init__(name, src, limit, origin, offset)
        import cv2
        self.frame = None
        self.cap = None
        self.test_limit = test_limit # only using this in experiment latency testing !!
        if len(self.src) < 2:
            raise CorruptedOrMissingVideo("At least 2 video files required to use HwangVideoStreams."
                                          "For single video, use HwangVideoStream instead.")
        self.caches = {}
        self.iters = {}
    
    def _cache(self, index):
        if index not in self.caches:
            video = HwangVideoStream(self.src[index], str(index))
            self.caches[index] = video.all()
            return True
        return False

    # only remove if there are no current iterators that need it
    def _remove_cache(self, index):
        remove_cache = True
        for op_name in self.iters:
            # index  should be video['index'] - 1
            if self.iters[op_name]['index'] == index:
                remove_cache = False
        if remove_cache:
            del self.caches[index]
        return remove_cache

    def add_iter(self, op_name):
        self.iters[op_name] = {}
        self.iters[op_name]['index'] = 0
        self.iters[op_name]['frame_count'] = 0
        self.iters[op_name]['curr_frame'] = -1
        self._cache(self.iters[op_name]['index']) 
        return self

    def next(self, op_name):
        video = self.iters[op_name]
        if self.limit > 0 and video['frame_count'] >= self.limit:
            self._remove_cache(video['index'])
            del self.iters[op_name]
            raise StopIteration("Iterator is closed")
        else:
            
            try:
                index = video['index']
                if video['curr_frame'] < len(self.caches[index]) - 1:     
                    video['curr_frame'] +=1
                    video['frame_count'] += 1
                    frame = self.caches[index][video['curr_frame']]
                
                else:
                    video['index'] += 1
                    if video['index'] == len(self.src):
                        self._remove_cache(index)
                        del self.iters[op_name]
                        raise StopIteration

                    self._cache(video['index'])
                    frame = self.caches[video['index']][0]
                    video['curr_frame'] = 0
                    video['frame_count'] += 1
                    self._remove_cache(index)

            except KeyError:
                raise KeyError("Cache logic failed - current indexed src not cached")




            #     # change vstreams
            #     if video['index'] + 1 < len(self.src):
            #         video['thread'].join()
            #         video['vstream'] = self.next_vstream
            #         frame = video['next_frame']
            #         video['frame_count'] += 1
            #         # start another thread
            #         video['next_frame'] = None
            #         video['next_vstream'] = None
            #         video['thread'] = threading.Thread(target=self._cache(op_name))
            #         video['thread'].start()
            #         video['index'] += 1
            #     else:
            #         raise StopIteration("Iterator is closed")
            # return self

    def get_vstream(self, op_name):
        return self.iters[op_name]['vstream']

class CVVideoStreams(VideoStream):
    def __init__(self, src, name, limit = -1, origin = np.array((0,0)), offset = 0, test_limit = -1):
        super().__init__(name, src, limit, origin, offset)
        import cv2
        self.frame = None
        self.cap = None
        self.test_limit = test_limit # only using this in experiment latency testing !!
        if len(self.src) < 2:
            raise CorruptedOrMissingVideo("At least 2 video files required to use CVVideoStreams."
                                          "For single video, use CVVideoStream instead.")
<<<<<<< HEAD
        self.iters = {}
=======
>>>>>>> 51edaebe6a0c97f8d9888608310806e8591597dd
    
    def _cache(self, op_name):
        next_cache = self.src[self.index + 1]
        self.iters[op_name]['next_vstream'] = CVVideoStream(next_cache, self.name, limit = self.test_limit).add_iter('main')
        self.iters[op_name]['next_frame'] = self.iters[op_name]['next_vstream'].next('main')

    def add_iter(self, op_name):
        self.iters[op_name] = {}
        self.iters[op_name]['index'] = 0
        self.iters[op_name]['frame_count'] = -1
        self.iters[op_name]['vstream'] = CVVideoStream(self.src[0], self.name, limit = self.test_limit, offset=self.offset).add_iter('main')
        self.iters[op_name]['thread'] = threading.Thread(target=self._cache(op_name))
        self.iters[op_name]['thread'].start()
        return self

    def next(self, op_name):
        video = self.iters[op_name]
        if self.limit > 0 and video['frame_count'] >= self.limit - 1:
            raise StopIteration("Iterator is closed")
        else:
            try:
<<<<<<< HEAD
                frame = video.next(op_name)
                video['frame_count'] += 1
            except StopIteration:
                # change vstreams
                if video['index'] + 1 < len(self.src):
                    video['thread'].join()
                    video['vstream'] = self.next_vstream
                    frame = video['next_frame']
                    video['frame_count'] += 1
=======
                self.frame = next(self.vstream).get()
                self.frame_count += 1
            except StopIteration:
                # change vstreams
                if self.index + 1 < len(self.src):
                    self.thread.join()
                    self.vstream = self.next_vstream
                    self.frame = self.next_frame
                    self.frame_count += 1
>>>>>>> 51edaebe6a0c97f8d9888608310806e8591597dd
                    # start another thread
                    video['next_frame'] = None
                    video['next_vstream'] = None
                    video['thread'] = threading.Thread(target=self._cache(op_name))
                    video['thread'].start()
                    video['index'] += 1
                else:
                    raise StopIteration("Iterator is closed")
            return frame
    
    def get_vstream(self, op_name):
        return self.iters[op_name]['vstream']
    
    @staticmethod
    def init_mat(file_name, encoding, width, height, frame_rate):
        fourcc = cv2.VideoWriter_fourcc(*encoding)
        writer = cv2.VideoWriter(file_name,
                        fourcc,
                        frame_rate,
                        (width, height),
                        True)
        return writer
        
    @staticmethod
    def append(data, prev):
        prev.write(data)
        return prev


