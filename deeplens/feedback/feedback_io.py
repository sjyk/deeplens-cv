"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

feedback_io.py defines an I/O class that are called by the StorageManager.
It allows users to insert feedback from VideoStream pipelines into permenent
storage.
"""

import json

class FeedbackIO():
    def __init__(self, conn, name, streams, physical_dir, materialize, args, width = None, height = None):
        self.conn = conn
        self.name = name
        self.streams = streams
        self.dir = physical_dir
        self.materialize = materialize
        self.args = args
        self.writers = {}
        self.batch_size = args['batch_size']
        self.batch_index = 0
        self.width = width
        self.height = height

        if self.materialize:
            self.writers['video'] = None
            if self.width == None or self.height == None:
                raise TypeError('we must have width/hieght when we materialize the video')
        for stream in self.streams:
            if streams[stream]:
                self.writers[stream] = None
            else:
                self.writers[stream] = []

    def add_frame(self, data):
        if self.batch_index == 0 and self.materialize:
            r_name = vid_name + get_rnd_strng(64)
            seg_name = os.path.join(self.dir, r_name)
            self.file_name = add_ext(seg_name, AVI)
            self.writers['video'] = cv2.VideoWriter(file_name,
                                    fourcc,
                                    frame_rate,
                                    (width, height),
                                    True)
        
        if self.materialize:
            self.writers.write(data['video'])
        
        for stream in self.streams:
            if self.streams[stream]:
                self.writers[stream] = data[stream]
            else:
                self.writers.append(data[stream])

        self.batch_index += 1
        if self.batch_index == self.batch_size:
            # update header for video -> can we have an empty clip? -> yes?
            for stream in self.streams:
                if self.streams[stream]:
                    pass
                    #update label with header
                else:
                    r_name = vid_name + get_rnd_strng(64)
                    seg_name = os.path.join(self.dir, r_name)
                    file_name = add_ext(seg_name, AVI)
                    with open(file_name, 'w') as f:
                        json.dump(file_name, f)
                    # update label with header
            for stream in streams:
                if self.streams[stream]:
                    self.writers[stream] = None
                else:
                    self.writers[stream] = []
                if materialize:
                    self.writers['video'] = None