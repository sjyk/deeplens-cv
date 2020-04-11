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

    def __init__(self, data_queue):
        self.data_queue = data_queue

    def __iter__(self):
        """Constructs the iterator object and initializes
           the iteration state
        """
        self.data_queue = iter(self.data_queue)
        return self

    def __getitem__(self, xform):
        """Applies a transformation to the pipeline
        """
        return xform.apply(self)

    def __next__(self):
        return next(self.data_queue)

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



