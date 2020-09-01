import json
from deeplens.utils.error import MissingIndex

class CacheStream(DataStream):
    def __init__(self, name, operator):
        super().__init__(name)
        self.data = []
        self.operator = iter(operator)
        self.size = 0
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
            if i < self.index + size:
                break
            next(self.operator)
        
        min_index = min(self.iters.values())
        if not self.keep_all:
            if min_index == i:
                while self.index < i:
                    self.size -= 1
                    self.index += 1
                    del self.data[0]
        
        return self.data[i - self.index]

    def insert(self, value):
        self.data.append(value)
        self.size +=1

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

# class CacheFullMetaStream(CacheStream):
#     def __init__(self, name, stream_type):
#         super().__init__(name, stream_type)
#         self.data = 0
#         self.name = name
#         self.vid_name = None
#         self.crops = None
#         self.video_refs = None
#         self.fcoor = None
#         self.scoor = None
#         self.first_frame = None
#         self.new_batch = None
#         self.do_join = None

#     def update(self, index, new_batch = False):
#         self.data = index
#         self.new_batch = new_batch

#     def update_all(self, index, vid_name, crops, video_refs, fcoor, scoor, do_join):
#         self.data = 0
#         self.name = vid_name
#         self.crops = crops
#         self.video_refs = video_refs
#         self.fcoor = fcoor
#         self.scoor = scoor
#         self.data = index
#         self.first_frame = index
#         self.new_batch = True
#         self.do_join = do_join