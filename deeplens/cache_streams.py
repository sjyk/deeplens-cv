import json
from deeplens.utils.error import MissingIndex

class CacheStream(DataStream):
    def __init__(self, name, operator):
        super().__init__(name)
        self.data = []
        self.operator = iter(operator)
        self.index = 0
        self.keep_all = False

    #only used by GraphManager -> to return results 
    #if you need to keep some datastream, set the results parameter on GraphManager run 
    def keep_all(self):
        self.keep_all = True

    def add_iter(self, op_name):
        self.iters[op_name] = -1
        return self
    
    def next(self, op_name):
        self.iters[op_name] += 1
        i = self.iters[op_name]
        while True:
            if i < self.index:
                raise MissingIndex('Cache index not saved')
            if i < self.index + len(self.data):
                break
            next(self.operator)
        
        min_index = min(self.iters.values())
        if not self.keep_all:
            if min_index == i:
                while self.index < i:
                    self.index += 1
                    del self.data[0]
        
        return self.data[i - self.index]

    def insert(self, value):
        self.data.append(value)

    def all(self, value):
        return self.data
    @staticmethod
    def init_mat():
        return []
    
    @staticmethod
    def append(data, prev):
        return prev.append(data)
    
    @staticmethod
    def materialize(data, fp = None):
        if not fp:
            return json.dumps(data)
        else:
            return json.dump(data, fp)