"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

pipeline.py defines the main data structures used in deeplens's pipeline. It defines a
video input stream as well as operators that can transform this stream.

Note this differs form struct.py because it separates the videostream from
the pipeline?
"""
import cv2
from deeplens.error import *
import numpy as np
import json
from timeit import default_timer as timer

#sources video from the default camera
DEFAULT_CAMERA = 0

class Pipeline():
    """The video stream class opens a stream of video
       from a source.

    Frames are structured in the following way: (1) each frame 
    is a dictionary where frame['data'] is a numpy array representing
    the image content, (2) all the other keys represent derived data.

    All geometric information (detections, countours) go into a list called
    frame['bounding_boxes'] each element of the list is structured as:
    (label, box).
    """

    def __init__(self, video_stream, query):
        """Constructs a videostream object

           Input: src- Source camera or file or url
                  limit- Number of frames to pull
                  origin- Set coordinate origin
        """
        self.vs = video_stream
        self.dss = []
        self.queries = query
        self.ds_queries = []

    def __iter__(self):
        """Constructs the iterator object and initializes
           the iteration state
        """
        if type(query) == list:
            self.vs.initialize(self.query[0])
            self.video_index = 0
            for i in range(len(self.dss)):
                self.dss[i].initialize(self.ds_queries[i][0])
        else:
            self.vs.initialize(self.query)
            for i in range(len(self.dss)):
                self.dss[i].initialize(self.ds_queries[i])
        return self

    def __getitem__(self, xform):
        """Applies a transformation to the pipeline
        """
        return xform.apply(self)

    def __next__(self):
        frame =  self.vs.next()
        if frame == None:
            if type(self.query) != list:
                return None
            elif self.video_index == len(self.query) - 1:
                return None
            else:
                video_index += 1
                self.vs.initialize(self.query[video_index])
                frame = self.vs.next()
                for i in range(len(self.dss)):
                    self.dss[i].initialize(self.ds_queries[i][video_index])              
        
        for ds in self.dss:
            frame.update(ds.next())
        return frame

    def lineage(self):
        return [self]

    def add_datastream(self, data_stream, query):
        """ Add an auxillary datastream. Note that the number of queries
        must be equal to tne number of video queries, and each query return
        the same number of frames of the matching video query. 
        """
        self.ds_queries.append(query)
        self.dss.append(data_stream)

class StoragePipeline():
    """The video stream class opens a stream of video
       from a source.

    Frames are structured in the following way: (1) each frame 
    is a dictionary where frame['data'] is a numpy array representing
    the image content, (2) all the other keys represent derived data.

    All geometric information (detections, countours) go into a list called
    frame['bounding_boxes'] each element of the list is structured as:
    (label, box).
    """

    def __init__(self, stmanager, video_stream, video_name, clips_ids, materialize = True):
        """Constructs a pipeline object linked with a FullManager
        """
        self.stmanager = stmanager
        self.vs = video_stream
        self.video_name = video_name
        self.clip_ids = clips_ids
        self.dss = {}
        self.ds_files = {}
        self.files = []
        self.labels = []
        for id in clip_ids:
            file = stmanager.get_clip_file(id)
            self.files.append(file)
    
    def __iter__(self):
        """Constructs the iterator object and initializes
           the iteration state
        """
        self.vs.initialize(self.files[0])
        self.video_index = 0
        for label in range(len(self.dss)):
            self.dss[label].initialize(self.ds_files[label][0])
        return self

    def __getitem__(self, xform):
        """Applies a transformation to the pipeline
        """
        return xform.apply(self)

    def __next__(self):
        frame =  self.vs.next()
        if frame == None:
            if self.video_index == len(self.clip_ids) - 1:
                return None
            else:
                video_index += 1
                self.vs.initialize(self.files[video_index])
                frame = self.vs.next()
                for label in self.labels:
                    self.dss[label].initialize(self.ds_files[label][video_index])              
        
        for ds in self.dss:
            frame.update(ds.next())
        
        return frame

    def lineage(self):
        return [self]

    def add_datastream(self, label,  data_stream):
        if label not in labels:
            self.labels.append(label)
            self.ds_files[label] = []
            for id in self.clip_ids:
                file = self.stmanager.get_clip_label(label, clip_id, name)
                self.ds_files[label].append(file)
            self.dss[label] = data_stream
            
# TODO: VideoStream types: Hwang, OpenCV, Iterator
# TODO: DataStream types: Constant, list
class IteratorVideoStream(VideoStream):
    """The video stream class opens a stream of video
       from an iterator over frames (e.g., a sequence
       of png files). Compatible with opencv streams.
    """

    def __init__(self, src, limit=-1):
        """Constructs a videostream object

           Input: src- iterator over frames
                  limit- Number of frames to pull
        """
        self.src = src
        self.limit = limit
        self.global_lineage = []

    def __getitem__(self, xform):
        """Applies a transformation to the video stream
        """
        return xform.apply(self)

    def __iter__(self):
        """Constructs the iterator object and initializes
           the iteration state
        """

        try:
            self.frame_iter = iter(self.src)
        except:
            raise CorruptedOrMissingVideo(str(self.src) + " is corrupted or missing.")

        try:
            self.next_frame = next(self.frame_iter)
            # set sizes after the video is opened
            if 'data' in self.next_frame:
                self.width = int(self.next_frame['data'].shape[0])  # float
                self.height = int(self.next_frame['data'].shape[1])  # float

            self.frame_count = 1
        except StopIteration:
            self.next_frame = None

        return self

    def __next__(self):
        if self.next_frame == None:
            raise StopIteration("Iterator is closed")

        if (self.limit < 0 or self.frame_count <= self.limit):
            ret = self.next_frame
            self.next_frame = next(self.frame_iter)

            if 'frame' in ret:
                return ret
            else:
                self.frame_count += 1
                ret.update({'frame': (self.frame_count - 1)})
                return ret
        else:
            raise StopIteration("Iterator is closed")

    def lineage(self):
        return self.global_lineage


class RawVideoStream(VideoStream):
    """The video stream class opens a stream of video
       from an iterator over frames (e.g., a sequence
       of png files). Compatible with opencv streams.
    """

    def __init__(self, src, limit=-1, origin=np.array((0,0))):
        """Constructs a videostream object

           Input: src- iterator over frames
                  limit- Number of frames to pull
        """
        self.src = src
        self.limit = limit
        self.global_lineage = []
        self.origin = origin

    def __getitem__(self, xform):
        """Applies a transformation to the video stream
        """
        return xform.apply(self)

    def __iter__(self):
        """Constructs the iterator object and initializes
           the iteration state
        """

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

    def __next__(self):
        if self.next_frame is None:
            raise StopIteration("Iterator is closed")

        if (self.limit < 0 or self.frame_count <= self.limit):
            ret = self.next_frame
            self.next_frame = next(self.frame_iter)
            self.frame_count += 1
            return {'frame': (self.frame_count - 1), 'data': ret, 'origin': self.origin}
        else:
            raise StopIteration("Iterator is closed")

    def lineage(self):
        return self.global_lineage

#given a list of pipeline methods, it reconstucts it into a stream
def build(lineage):
    """build(lineage) takes as input the lineage of a stream and
    constructs the stream.
    """
    plan = lineage
    if len(plan) == 0:
        raise ValueError("Plan is empty")
    elif len(plan) == 1:
        return plan[0]
    else:
        v = plan[0]
        for op in plan[1:]:
            v = v[op]
        return v


class Operator():
    """An operator defines consumes an iterator over frames
    and produces and iterator over frames. The Operator class
    is the abstract class of all pipeline components in dlcv.
    
    We overload python subscripting to construct a pipeline
    >> stream[Transform()] 
    """

    #subscripting binds a transformation to the current stream
    def apply(self, pipeline):
        self.pipeline = pipeline
        return self

    def __getitem__(self, xform):
        """Applies a transformation to the video stream
        """
        return xform.apply(self)

    def lineage(self):
        """lineage() returns the sequence of transformations
        that produces the given stream of data. It can be run
        without materializing any of the stream.

        Output: List of references to the pipeline components
        """
        if isinstance(self.pipeline, Pipeline):
            return [self.pipeline, self]
        else:
            return self.pipeline.lineage() + [self]

    def _serialize(self):
        return NotImplemented("This operator cannot be serialized")

    def serialize(self):
        try:
            import json
            return json.dumps(self._serialize())
        except:
            return ManagerIOError("Serialization Error")



