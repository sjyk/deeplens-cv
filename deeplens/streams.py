# TODO: VideoStream types: Hwang, OpenCV, Iterator
# TODO: Add error if next is called before initialize
import json
import cv2
from timeit import default_timer as timer

class DataStream():
    def initialize(self, file):
        raise NotImplemented("initialize not implemented")
    def next(self):
        raise NotImplemented("next not implemented")

class JSONListDataStream(DataStream):
    def initialize(self, files):
        self.data = []
        for file in files:
            with open(file, 'r') as f:
                self.data.append(iter(json.load(f)))

    def next(self):
        frame = []
        for d in self.data:
            frame.append(next(d))
        return frame

class ConstantDataStream(DataStream):
    def initialize(self, constants):
        self.data = constants

    def next(self):
        return constants

class VideoStream(DataStream):
    def __init__(self, limit=-1):
        self.limit = limits
    
    def initialize(self, src, origin=np.array((0,0)), offset = 0):
        raise NotImplemented("initialize not implemented")


class CVVideoStream(VideoStream):
    def __init__(self, limit = -1):
        super.__init__(limit)
        self.propIds = None

    def initialize(self, src, origin=np.array((0,0)), offset = 0):
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
                return {'data': frame, \
                        'frame': (self.frame_count - 1),\
                        'origin': self.origin,
                        'width': self.width,
                        'height': self.hei}
            else:
                return None
        else:
            # self.cap.release()  # commented out due to CorruptedOrMissingVideo error
            self.cap = None
            return None
    
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


class RawVideoStream(VideoStream):
    def __init__(self, limit=-1):
        super.__init__(limit)
    
    def initialize(self, src, origin=np.array((0,0)), offset = 0):
        self.origin = origin
        try:
			self.frame_iter = iter(self.src)
        except:
			raise CorruptedOrMissingVideo(str(self.src) + " is corrupted or missing.")
        try:
			self.next_frame = next(self.frame_iter)
			# set sizes after the video is opened
			self.width = int(self.next_frame.shape[0])  # float
			self.height = int(self.next_frame.shape[1])  # float
			self.frame_count = 1
        except StopIteration:
			self.next_frame = None
        return self
    
    def next(self):
		if self.next_frame is None:
			return None

		if (self.limit < 0 or self.frame_count <= self.limit):
			ret = self.next_frame
			self.next_frame = next(self.frame_iter)
			self.frame_count += 1
			return {'frame': (self.frame_count - 1),
                    'data': ret,
                    'origin': self.origin,
                    'width': self.width,
                    'height': self.height}
		else:
			return None

# TODO: Add implementation with hwang backend
class HwangVideoStream(VideoStream):
    def __init__(self, limit=-1):
        super.__init__(limit)

    def initialize(self, src, origin=np.array((0,0)), offset = 0):
        raise NotImplemented("initialize not implemented")