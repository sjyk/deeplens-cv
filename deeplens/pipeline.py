"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

pipeline.py defines the main data structures used in deeplens's pipeline. It defines a
video input stream as well as operators that can transform this stream.
"""
import logging

from deeplens.error import *
from queue import Queue, Empty

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

    def __init__(self, vstream, dstreams):
        """Constructs a videostream object

           Input: src- Source camera or file or url
                  limit- Number of frames to pull
                  origin- Set coordinate origin
        """
        self.vstream = video_stream
        self.dstreams = datastreams

    def __iter__(self):
        """Constructs the iterator object and initializes
           the iteration state
        """
        self.vstream = iter(self.vstream)
        for i in range(len(self.dstreams)):
            self.dstreams[i] = iter(self.dstreams[i])
        return self

    def __getitem__(self, xform):
        """Applies a transformation to the pipeline
        """
        return xform.apply(self)

    def __next__(self):
        frame =  next(self.vstream)
        for ds in self.dstreams:
            frame.update(next(ds))
        return frame

    def lineage(self):
        return [self]


class PipelineManager():
    """The video stream class opens a stream of video
       from a source.

    Frames are structured in the following way: (1) each frame 
    is a dictionary where frame['data'] is a numpy array representing
    the image content, (2) all the other keys represent derived data.

    All geometric information (detections, countours) go into a list called
    frame['bounding_boxes'] each element of the list is structured as:
    (label, box).
    """
    def __init__(self):
        self.operators = []
        self.vstream = None
        self.dstreams = []

    def get_operators(self):
        return self.operators
    
    def update_operators(self, operators):
        self.operators = operators

    def build(self):
        pipeline = Pipeline(self.vstream, self.dstreams)
        for op in self.operators:
            pipeline = pipeline[op]
        return pipeline

    def add_operator(self, operator):
        self.operators.append(operator)

    def add_videostream(self, vstream):
        if self.vstream:
            self.vstream = vstream
            return (None, self.vstream)
        else:
            v = self.vstream
            self.vstream = vstream
            return (v, self.vstream)
    
    def add_datastream(self, datastream):
        self.dstreams.append(datastream)
        


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

    def __init__(self):
        """Constructs a pipeline object linked with a FullManager
        """
        self.video_stream = Queue()
        self.data_streams = {}
        self.labels = []
    
    def __iter__(self):
        """Constructs the iterator object and initializes
           the iteration state
        """
        return self

    def __getitem__(self, xform):
        """Applies a transformation to the pipeline
        """
        return xform.apply(self)

    def __next__(self):
        # Get VideoStream from queue
        try:
            current_videostream = self.dequeue_videostream()
        except Empty():
            logging.debug("self.video_stream queue is empty!")
            raise StopIteration("Iterator is closed")

        # Get DataStream from queue
        try:
            current_datastreams = [self.dequeue_datastream(label) for label in self.labels]
        except Empty():
            logging.debug("self.data_streams queue is empty!")
            raise StopIteration("Iterator is closed")

        return (current_videostream, current_datastreams)

    def add_datastream(self, label, data_stream):
        if label in self.labels:
            raise KeyError("Label '%s' already in pipeline!" % label)

        self.labels.append(label)

        self.data_streams[label] = Queue()
        self.data_streams[label].put(data_stream)

    def enqueue_datastream(self, label, data_stream):
        if label not in self.labels:
            raise KeyError("Label '%s' does not exist in pipeline!" % label)

        self.data_streams[label].put(data_stream)

    def dequeue_datastream(self, label):
        return self.data_streams[label].get_nowait()

    def enqueue_videostream(self, video_stream):
        self.video_stream.put(video_stream)

    def dequeue_videostream(self):
        return self.video_stream.get_nowait()

    def lineage(self):
        return [self]


class PipelineOperator():
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
        return NotImplementedError("This operator cannot be serialized")

    def serialize(self):
        try:
            import json
            return json.dumps(self._serialize())
        except:
            return ManagerIOError("Serialization Error")



