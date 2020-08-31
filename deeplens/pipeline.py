"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

pipeline.py defines the main data structures used in deeplens's pipeline. It defines a
video input stream as well as operators that can transform this stream.
"""
import logging
import networkx as nx
from deeplens.utils.error import *
from queue import Queue, Empty
from deeplens.streams import *

#sources video from the default camera
DEFAULT_CAMERA = 0

class Operator():
    """An operator defines consumes an iterator over frames
    and produces and iterator over frames. The Operator class
    is the abstract class of all pipeline components in dlcv.
    """
    # need to initialize appropriate dstreams
    def __init__(self, name):
        self.name = name
        self.id

    def __iter__(self):
        raise NotImplemented("__iter__ implemented")

    def __next__(self):
        return self

    #binds previous operators and dstreams to the current stream
    def apply(self, streams):
        self.streams = streams
        for stream in self.streams:
            self.streams[stream].add_iter(name)

class GraphManager():
    """ Creates and manages a DAG graph for pipeline purposes
    NOTE: We don't really error check for duplicate streams
    """
    def __init__(self):
        self.roots = {}
        self.dstreams = {}
        self.graph = nx.DiGraph()
        self.leaves = {}

    def get_operators(self):
        return self.operators.keys()

    def get_leaves(self):
        return self.leaves.keys()

    def build(self):
        mat_streams = {}
        for name in self.graph.nodes:
            dstreams = self.graph[name]['dstreams']
            parents = self.graph[name]['parents']
            curr_streams = {}
            for stream in dstreams:
                curr_streams[stream] = self.dstreams[stream]
            for par in parents:
                curr_streams.update(self.graph[par]['streams'])
            self.graph[name]['operator'].apply(curr_streams)
            iter(self.graph[name]['operator'])

    # denote which leaves to run
    def run(self, plan = None):
        run_streams = {}
        if plan != None:
            while True:
                finished = True
                for (name, num) in plan:
                    for i in range(num):
                        try:
                            next(self.graph[name]['operator'])
                            finished = False
                        except StopIteration:
                            break
                if finished:
                    break
        else:
            while True:
                finished = True
                for name in self.leaves:
                    try:
                        next(self.graph[name]['operator'])
                        finished = False
                    except StopIteration:
                        break
                if finished:
                    break
    # do not allow overwriting -> maybe change later
    def add_operator(self, operator, dstreams = None, parents = None):
        nodes = set(self.graph.nodes)
        name = operator.name
        if name in self.graph.nodes:
            return False
        elif parents and not nodes.issuperset(set(parents)):
            return False
            
        elif dstreams and set(self.dstreams.keys()).issuperset(dstreams):
            return False
        else:
            self.graph.add_node((name, {'dstreams': dstreams, 'parents': parents, 'operator': operator, 'streams': operator.streams}))
            if parents == None:
                self.roots.add(name)
            self.leaves.add(name)
            self.roots.difference(set(parents))
    
    def add_stream(self, datastream, stream_name):
        self.dstreams[stream_name] = datastream

    def add_streams(self, datastreams):
        self.dstreams.update(datastreams)
    
    def clear_streams(self):
        dstreams = self.dstreams
        self.dstreams = {}
        return dstreams
