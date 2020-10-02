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
import copy

#sources video from the default camera
DEFAULT_CAMERA = 0

class Operator():
    """An operator defines consumes an iterator over frames
    and produces and iterator over frames. The Operator class
    is the abstract class of all pipeline components in dlcv.
    """
    # need to initialize appropriate dstreams
    def __init__(self, name, input_names, output_names):
        self.input_names = input_names
        self.name = name
        self.output_names = output_names
        self.results = {}

    def __iter__(self):
        self.index = -1
        return self

    def __next__(self):
        #print(self.name)
        self.index += 1

    #binds previous operators and dstreams to the current stream
    def apply(self, streams):
        self.streams = streams
        for stream in self.streams:
            self.streams[stream].add_iter(self.name)

class GraphManager():
    """ Creates and manages a DAG graph for pipeline purposes
    NOTE: We don't safety check for duplicate streams/operators !!!!
    """
    def __init__(self):
        self.dstreams = {}
        self.graph = nx.DiGraph()
        self.leaves = []

    def get_operators(self):
        return self.operators.keys()

    def get_leaves(self):
        return self.leaves

    def build(self):
        mat_streams = {}
        for name in self.graph.nodes:
            op = self.graph.nodes[name]['attr_dict']['operator']
            streams = op.input_names
            curr_streams = {}
            for stream in streams:
                curr_streams[stream] = self.dstreams[stream]
            op.apply(curr_streams)
            iter(op)

    # denote which leaves to run
    def run(self, leaves, plan = None, results = []):
        run_streams = {}
        self.build()
        if  plan != None:
            while True:
                finished = True
                for (name, num) in plan:
                    for i in range(num):
                        try:
                            next(self.graph.nodes[name]['attr_dict']['operator'])
                            finished = False
                        except StopIteration:
                            break
                if finished:
                    break
        else:
            if leaves == 'all':
                leaves = copy.copy(self.graph.nodes.keys())
            ops = copy.copy(leaves)
            i = 0
            while True:
                fops = []
                print(i)
                if i % 100 == 0:
                    print(i, flush = True)
                for name in ops:
                    try:
                        next(self.graph.nodes[name]['attr_dict']['operator'])
                        finished = False
                    except StopIteration:
                        fops.append(name)
                        continue
                for op in fops:
                    ops.remove(op)
                if len(ops) == 0:
                    break
                i +=1
        output = {}
        for result in results:
            output[result] = self.dstreams[result].all()
        
        return output

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
        elif dstreams and not set(self.dstreams.keys()).issuperset(dstreams):
            return False
        else:
            self.graph.add_node(name, attr_dict={'operator': operator})
            #self.leaves.append(name)
            if operator.results != None:
                self.dstreams.update(operator.results)
            return True
    
    def add_stream(self, datastream):
        name = datastream.name
        self.dstreams[name] = datastream

    def add_streams(self, datastreams):
        self.dstreams.update(datastreams)
    
    def clear_streams(self):
        dstreams = self.dstreams
        self.dstreams = {}
        return dstreams
