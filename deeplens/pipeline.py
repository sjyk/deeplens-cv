"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

pipeline.py defines the main data structures used in deeplens's pipeline. It defines a
video input stream as well as operators that can transform this stream.
"""
import logging
import networkx as nx
from deeplens.utils.error import *
from deeplens.streams import *
import matplotlib.pyplot as plt

#sources video from the default camera
DEFAULT_CAMERA = 0

class Operator():
    """An operator defines consumes an iterator over frames
    and produces and iterator over frames. The Operator class
    is the abstract class of all pipeline components in dlcv.
    """
    # need to initialize appropriate dstreams
    def __init__(self, name, input_names):
        self.input_names = input_names
        self.name = name

    def __iter__(self):
        self.index = -1

    def __next__(self):
        self.index += 1

    #binds previous operators and dstreams to the current stream
    def apply(self, streams):
        self.streams = streams
        for stream in self.streams:
            self.streams[stream].add_iter(name)

class GraphManager():
    """ Creates and manages a DAG graph for pipeline purposes
    NOTE: We don't safety check for duplicate streams/operators !!!!
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
            dstreams = self.graph[name]['in_streams']
            parents = self.graph[name]['parents']
            curr_streams = {}
            for stream in dstreams:
                curr_streams[stream] = self.dstreams[stream]
            self.graph[name]['operator'].apply(curr_streams)
            iter(self.graph[name]['operator'])

    # denote which leaves to run
    def run(self, plan = None, results = None):
        run_streams = {}
        if  plan != None:
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
        
        output = {}
        for result in results:
            output[result] = self.dstreams[result].all()
        
        return result

    def draw(self):
        nx.draw(self.graph)
        plt.show()

    # do not allow overwriting -> maybe change later
    def add_operator(self, operator):
        nodes = set(self.graph.nodes)
        name = operator.name
        dstreams = set(operator.input_names)
        if name in self.graph.nodes:
            return False
        elif parents and not nodes.issuperset(set(parents)):
            return False
            
        elif dstreams and not set(self.dstreams.keys()).issuperset(dstreams):
            return False
        else:
            self.graph.add_node((name, {'in_streams': dstreams, 'operator': operator, 'out_streams': operator.results}))
            if parents == None:
                self.roots.add(name)
            self.leaves.add(name)
            self.roots.difference(set(parents))
            if operator.results != None:
                self.dstreams.update(operator.results)
    
    def add_stream(self, datastream):
        name = datastream.name
        self.dstreams[name] = datastream

    def add_streams(self, datastreams):
        self.dstreams.update(datastreams)
    
    def clear_streams(self):
        dstreams = self.dstreams
        self.dstreams = {}
        return dstreams
