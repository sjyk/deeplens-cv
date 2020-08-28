import os
import shutil
import sys
import pandas as pd
from timeit import default_timer as timer

import environ
from environ import *

from deeplens.constants import *
from deeplens.full_manager.condition import Condition
from deeplens.full_manager.full_manager import FullStorageManager
from deeplens.full_manager.full_video_processing import NullSplitter
from deeplens.utils.utils import get_local_ip


def runFullPut(src):
    local_folder = '/var/www/html/videos'
    ip_addr = get_local_ip()
    remote_folder = 'http://' + ip_addr + '/videos'
    manager = FullStorageManager(None, NullSplitter(), local_folder, remote_folder,
                                 dsn='dbname=header user=postgres password=deeplens host=10.0.0.5')

    def put():
        now = timer()
        manager.put(src, os.path.basename(src),
                    args={'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1})
        put_time = timer() - now
        logrecord('full', ({'file': src}), 'put', str({'elapsed': put_time}), 's')

    def get():
        clips = manager.get('test', Condition(label='foreground', custom_filter=None))
        now = timer()
        frame_count = 0
        for c in clips:
            for frame in c:
                frame_count += 1
        result = timer() - now
        logrecord('full', ({'file': src, 'frames': frame_count}), 'get', str(result), 's')

    put()

df = pd.read_csv('http://10.0.0.5/train/' + get_local_ip() + '.csv', sep=',',
                 dtype={'youtube_id': str})
youtube_ids=df['youtube_id']
youtube_ids2=list(dict.fromkeys(youtube_ids))

total_start = timer()
for item in youtube_ids2:
    try:
        video_path="http://10.0.0.5/train/"+item+".mp4"
        runFullPut(video_path)
    except:
        print("missing file for full", item)
print("Total put time on worker %s):" % get_local_ip(), timer() - total_start)
