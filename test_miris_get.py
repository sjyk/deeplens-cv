from test_miris import *
from deeplens.utils.testing_utils import *
import os
import time
from miris_splitters import *
# get queries -> requires to return a list of clip_ids 
#TEST_QUERIES = {"SELECT * FROM clip WERE is_background = 0"}
TEST_QUERIES = {}

# type of videos used from the miris dataset
VID_TYPES = ['beach', 'shibuya', 'uav', 'warsaw']
#VID_TYPES = ['shibuya']

#directory of the storage manager
BASE_STORAGE_DIR = '../miris_test_'
BASE_VIDEO_DIR = '../miris-data/'

#SPLITTERS = {'track': TrackSplitter()}
SPLITTERS = {'track': TrackSplitter(), 'area7': AreaSplitter(0.7), 'area3': AreaSplitter(0.3), 'area1': AreaSplitter(0.1), 'area7': AreaSplitter(0), 
            'tarea7': AreaTrackSplitter(0.7), 'tarea3': AreaTrackSplitter(0.3), 'tarea1': AreaTrackSplitter(0.1), 'tarea7': AreaTrackSplitter(0)}

# need to fix get queries
def main(base_dir, storage_dir):
    managers = {}
    for splitter in SPLITTERS:
        dire = storage_dir + splitter
        managers[splitter] = FullStorageManager(miris_tagger, SPLITTERS[splitter], dire)
    
    for query in TEST_QUERIES:
        for manager in managers:
            print('~~' + query + '~~')
            start_query = time.time()
            vstreams, clip_ids = managers[manager].get_vstreams(query)
            print(clip_ids)
            end_query = time.time()
            print("query time: " + str(end_query - start_query))
            total_pixels = 0
            start_vstream = time.time()
            for frame in vstreams:
                total_pixels += frame.width * frame.height
                frame.get()
            end_vstream = time.time()
            print("vstream time: " + str(end_vstream - start_vstream))
            print("total pixels: " + str(total_pixels))



if __name__ == '__main__':
    main(BASE_VIDEO_DIR, BASE_STORAGE_DIR)