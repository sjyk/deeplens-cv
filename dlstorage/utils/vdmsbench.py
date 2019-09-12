from dlstorage.filesystem.manager import *
from dlstorage.constants import *
from dlstorage.utils.debug import *
from dlstorage.VDMSsys.VDMSmanager import *
from dlstorage.utils.benchmark import *
import json

import os
from multiprocessing import Pool

class VDMSPerfTest(PerformanceTest):
    
    def __init__(self, storage_manager, test_video):
        self.sm = storage_manager
        self.test_video = test_video
        self.cnt = 0
    
    def putEncodingOneMin(self):
        for enc in ENCODINGS:
            args = {'encoding': enc, 'size': -1, 'limit': 60*DEFAULT_FRAME_RATE, 'sample': 1.0}

            #time put
            now = time.time()
            tname = 'test' + str(self.cnt)
            self.sm.put(self.test_video, tname, args)
            time_result = (time.time() - now)
            size_result = self.sm.size(tname)

            log = {'time': time_result, 'space': size_result}
            log.update(args)

            self.sm.delete(tname)
            os.system("sh cleandir.sh")

            print(json.dumps(log))
            self.cnt += 1

    def putClipOneMin(self):
        for size in range(5,60,5):
            args = {'encoding': XVID, 'size': size, 'limit': 60*DEFAULT_FRAME_RATE, 'sample': 1.0}

            #time put
            now = time.time()
            tname = 'test' + str(self.cnt)
            self.sm.put(self.test_video, tname, args)
            time_result = (time.time() - now)
            size_result = self.sm.size(tname)

            log = {'time': time_result, 'space': size_result}
            log.update(args)

            self.sm.delete(tname)
            os.system("sh cleandir.sh")

            print(json.dumps(log))
            self.cnt += 1
            
    def getClipSizeOneMin(self):
        for size in range(5,60,5):
            args = {'encoding': XVID, 'size': size, 'limit': 60*DEFAULT_FRAME_RATE, 'sample': 1.0}

            #time put
            tname = 'test' + str(self.cnt)
            self.sm.put(self.test_video, tname, args)

            now = time.time()
            time_result = timeof(self.sm.get(tname, TRUE, int(size*DEFAULT_FRAME_RATE)))
            full_time_result = (time.time() - now)
            log = {'time': time_result,'first_frame': full_time_result - time_result, 'retr_clip_size': size}
            log.update(args)

            self.sm.delete(tname)
            os.system("sh cleandir.sh")

            print(json.dumps(log))
            self.cnt += 1

    def getClipSizeTenSec(self):
        for size in range(5,60,5):
            args = {'encoding': XVID, 'size': size, 'limit': 60*DEFAULT_FRAME_RATE, 'sample': 1.0}

            #time put
            tname = 'test' + str(self.cnt)
            self.sm.put(self.test_video, tname, args)

            now = time.time()
            time_result = timeof(self.sm.get(tname, TRUE, int(10*DEFAULT_FRAME_RATE)))
            full_time_result = (time.time() - now)
            log = {'time': time_result,'first_frame': full_time_result - time_result, 'retr_clip_size': 10}
            log.update(args)

            self.sm.delete(tname)
            os.system("sh cleandir.sh")

            print(json.dumps(log))
            self.cnt += 1

    def getEncTenTenSec(self):
        for enc in ENCODINGS:
            args = {'encoding': enc, 'size': 10, 'limit': 60*DEFAULT_FRAME_RATE, 'sample': 1.0}

            #time put
            tname = 'test' + str(self.cnt)
            self.sm.put(self.test_video, tname, args)

            now = time.time()
            time_result = timeof(self.sm.get(tname, TRUE, int(10*DEFAULT_FRAME_RATE)))
            full_time_result = (time.time() - now)
            log = {'time': time_result,'first_frame': full_time_result - time_result, 'retr_clip_size': 10}
            log.update(args)

            self.sm.delete(tname)
            os.system("sh cleandir.sh")

            print(json.dumps(log))
            self.cnt += 1


    def getSelTenTenSec(self):
        for size in range(0,70,10):
            args = {'encoding': XVID, 'size': 10, 'limit': 60*DEFAULT_FRAME_RATE, 'sample': 1.0}

            #time put
            tname = 'test' + str(self.cnt)
            self.sm.put(self.test_video, tname, args)
            now = time.time()
            time_result = timeof(self.sm.get(tname, startsBefore(size*DEFAULT_FRAME_RATE), int(10*DEFAULT_FRAME_RATE)))
            full_time_result = (time.time() - now)
            log = {'time': time_result,'first_frame': full_time_result - time_result, 'retr_clip_size': size, 'sel': size/60}
            log.update(args)

            self.sm.delete(tname)
            os.system("sh cleandir.sh")

            print(json.dumps(log))
            self.cnt += 1


    def getParaTenTenSec(self):
        for para in range(1,5):
            args = {'encoding': XVID, 'size': 10, 'limit': 60*DEFAULT_FRAME_RATE, 'sample': 1.0}

            #time put
            tname = 'test' + str(self.cnt)
            self.sm.put(self.test_video, tname, args)

            now = time.time()
            time_result = timeof(self.sm.get(tname, TRUE, int(10*DEFAULT_FRAME_RATE), threads=Pool(para)))
            full_time_result = (time.time() - now)
            log = {'time': time_result,'first_frame': full_time_result - time_result, 'retr_clip_size': 10, 'para': para}
            log.update(args)

            self.sm.delete(tname)
            os.system("sh cleandir.sh")

            print(json.dumps(log))
            self.cnt += 1


    def runAll(self):
        print('[dlstorage] put() for different encodings and a video of 1 min')
        self.putEncodingOneMin()
        print('[dlstorage] put() for MP4V and varying clip size for a video of 1 min')
        self.putClipOneMin()
        print('[dlstorage] get() for MP4V full video of different clip sizes')
        self.getClipSizeOneMin()
        print('[dlstorage] get() for MP4V 10 sec clips of different get clip sizes')
        self.getClipSizeTenSec()
        print('[dlstorage] get() for different encodings 10 sec clips of different 10 sec sizes')
        self.getEncTenTenSec()
        print('[dlstorage] get() for different selectivities 10 sec clips of different 10 sec sizes')
        self.getSelTenTenSec()
        print('[dlstorage] get() for different number of threads 10 sec clips of different 10 sec sizes')
        self.getSelTenTenSec()

    

