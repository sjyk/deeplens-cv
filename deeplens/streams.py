# TODO: VideoStream types: Hwang, OpenCV, Iterator
# TODO: Add error if next is called before initialize
import json
import os
import tempfile
from shutil import copyfile
from subprocess import Popen
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
        return self
    
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
    
    @staticmethod
    def init_mat():
        return []

    @staticmethod
    def append(data, prev):
        return prev.append(data)

    @@staticmethod
    def materialize(data, fp = None):
        if not file_name:
            return json.dumps(data)
        else:
            return json.dump(data, fp)

class ConstantStream(DataStream):
    def __init__(self, data, name):
        super.__init__(name)
        self.data = data
    
    def get(self):
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

class CacheStream(DataStream):
    def __init__(self, name):
        super.__init__(name)
        self.data = None
    
    def get(self):
        return self.data

    def update(self, data):
        self.data = data

    @staticmethod
    def init_mat():
        return []
    
    @staticmethod
    def append(data, prev):
        return prev.append(data)
    
    @staticmethod
    def materialize(data, fp = None):
        if not file_name:
            return json.dumps(data)
        else:
            return json.dump(data, fp)

class CacheFullMetaStream(CacheStream):
    def __init__(self, name):
        super.__init__(name)
        self.data = 0
        self.name = name
        self.vid_name = None
        self.crops = None
        self.video_refs = None
        self.fd = None
        self.sd = None
        self.first_frame = None
        self.new_batch = None

    def update(self, index, new_batch = False):
        self.data = index
        self.new_batch = new_batch

    def update_all(self, index, vid_name, crops, video_refs, fcoor, scoor, joined):
        self.data = 0
        self.name == vid_name
        self.crops == crops
        self.video_refs = video_refs
        self.fd = (fw, fh)
        self.sd = (sw, sh)
        self.data = index
        if not joined:
            self.first_frame = index
        self.new_batch = True
    

class VideoStream(DataStream):
    def __init__(self, src, name, limit=-1, origin = np.array((0,0)), offset = 0, start_time = 0):
        super.__init__(name)
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
        super.__init__(name, src, limit, origin, offset)
        import cv2
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

    def __next__(self):
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

    @staticmethod
    def init_mat(file_name, encoding, width, height, frame_rate):
        fourcc = cv2.VideoWriter_fourcc(*encoding)
        cv2.VideoWriter(file_name,
                        fourcc,
                        frame_rate,
                        (width, height),
                        True)
        
    @staticmethod
    def append(data, prev):
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
    def __init__(self, src, name, limit=-1, origin=np.array((0, 0)), rows=[]):
        super().__init__(src, limit, name, origin)
        import hwang
        self.rows = rows
        self.frame_count = 0
        self.frame = None
        self.decoder = hwang.Decoder(self.src)

    def __iter__(self):
        self.width = self.decoder.video_index.frame_width()
        self.height = self.decoder.video_index.frame_height()

        # TODO(swjz): fetch all rows when (limit == -1)
        self.frames = iter(self.decoder.retrieve(self.rows))

    def __next__(self):
        self.frame_count += 1
        self.frame = {'data': next(self.frames),
                      'frame': (self.frame_count - 1),
                      'origin': self.origin}

    def get(self):
        return self.frame

    def init_mat(self):
        raise NotImplemented("init_mat not implemented")

    def append(self, data, prev):
        raise NotImplemented("append not implemented")

    def materialize(self, data):
        raise NotImplemented("materialize not implemented")

    def _write_mp4(self, paths, output_name, fps, scale=None):
        # Adapted from https://github.com/scanner-research/scanner/blob/645fb2/python/scannerpy/column.py
        temp_paths = []
        for _ in range(len(paths)):
            fd, p = tempfile.mkstemp()
            os.close(fd)
            temp_paths.append(p)
        # Copy all files locally before calling ffmpeg
        for in_path, temp_path in zip(paths, temp_paths):
            copyfile(in_path, temp_path)
        files = '|'.join(temp_paths)

        args = ''
        if scale:
            args += '-filter:v "scale={:d}x{:d}" '.format(scale[0], scale[1])
        encode_lib = 'libx264' # Assume the video is encoded as H264 (potentially supports H265)

        cmd = (
            'ffmpeg -y '
            '-r {fps:f} '  # set the input fps
            '-i "concat:{input_files:s}" '  # concatenate the h264 files
            '-c:v {encode_lib:s} '
            '-filter:v "setpts=N" '  # h264 does not have pts' in it
            '-loglevel panic '
            '{extra_args:s}'
            '{output_name:s}.mp4'.format(
                input_files=files,
                fps=fps,
                extra_args=args,
                output_name=output_name,
                encode_lib=encode_lib))
        rc = Popen(cmd, shell=True).wait()
        if rc != 0:
            raise FFMpegError('ffmpeg failed during mp4 export!')