"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

pipeline.py defines the main data structures used in deeplens's pipeline. It defines a
video input stream as well as operators that can transform this stream.
"""
import logging

from deeplens.utils.error import *
from queue import Queue, Empty
from deeplens.streams import *

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

    def __init__(self, streams, pipeline = {}):
        """Constructs a videostream object

           Input: src- Source camera or file or url
                  limit- Number of frames to pull
                  origin- Set coordinate origin
        """
        self.streams = streams
        self.pipelines = pipeline

    def __iter__(self):
        """Constructs the iterator object and initializes
           the iteration state
        """
        for stream in self.streams:
            self.streams[stream] = iter(self.streams[stream])

        for pipeline in self.pipelines:
            iter(self.pipelines[pipeline])
        return self

    def __getitem__(self, xform):
        """Applies a transformation to the pipeline
        """
        return xform.apply(self)

    def __next__(self):
        for pipeline in self.pipelines:
            next(self.pipelines[pipeline])
        for stream in self.streams:
            self.streams[stream] = next(self.streams[stream])
        return self.streams

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
    def __init__(self, vstream):
        self.operators = []
        self.vstream = vstream
        self.dstreams = {}
        self.pipelines = {}

    def get_operators(self):
        return self.operators
    
    def update_operators(self, operators):
        self.operators = operators

    def build(self):
        if self.vstream == None:
            raise MissingVideoStream()
        streams = {'video': self.vstream}
        streams.update(self.dstreams)
        if len(self.pipelines) == 0:
            pipeline = Pipeline(streams)
        else:
            pipeline = Pipeline(streams, self.pipelines)
        for op in self.operators:
            pipeline = pipeline[op]
        return pipeline

    def run(self, result = None):
        pipeline = self.build()
        results = []            
        for frame in pipeline:
            if result:
                frame = frame[result].get()
                if type(frame) == DataStream:
                    frame = frame.materialize(frame)
                results.append(frame)
        return results

    def add_operator(self, operator):
        self.operators.append(operator)
    
    def add_operators(self, operators):
        self.operators = self.operators  + operators

    def update_videostream(self, vstream):
        if self.vstream:
            self.vstream = vstream
            return (None, self.vstream)
        else:
            v = self.vstream
            self.vstream = vstream
            return (v, self.vstream)
    
    def add_stream(self, datastream, stream_name):
        self.dstreams[stream_name] = datastream

    def add_streams(self, datastreams):
        self.dstreams.update(datastreams)
    
    def clear_streams(self):
        vstream = self.vstream
        dstreams = self.dstreams
        self.vstream = None
        self.dstreams = {}
        return(vstream, dstreams)

    def add_pipeline(self, pipeline, name):
        self.pipelines[name] = pipeline
        self.dstreams.update(pipeline.streams)

class Operator():
    """An operator defines consumes an iterator over frames
    and produces and iterator over frames. The Operator class
    is the abstract class of all pipeline components in dlcv.
    
    We overload python subscripting to construct a pipeline
    >> stream[Transform()] 
    """

    #subscripting binds a transformation to the current stream
    def apply(self, pipeline):
        self.pipeline = iter(pipeline)
        self.streams = self.pipeline.streams
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



