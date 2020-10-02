from test_miris import *
from deeplens.utils.testing_utils import *
import os
import time
from miris_splitters import *
import logging

# get queries -> requires to return a list of clip_ids 
#TEST_QUERIES = {"SELECT * FROM clip WERE is_background = 0"}
TEST_QUERIES = ["SELECT label.clip_id, fwidth, fheight FROM label LEFT JOIN clip ON label.clip_id = clip.clip_id WHERE label.video_name='{}'"]

# type of videos used from the miris dataset
VID_NAMES = ['warsaw5', 'uav0008', 'beach4', 'warsaw3', 'shibuya0', 'warsaw0', 'shibuya5', 'beach1', 'beach5', 'beach2', 'warsaw4', 'uav0009', 'warsaw1', 'beach0',
        'beach3', 'warsaw2', 'shibuya3', 'shibuya4', 'shibuya1', 'shibuya2', 'uav0006', 'uav0011', 'uav0007']

#VID_TYPES = ['shibuya']

#directory of the storage manager
BASE_STORAGE_DIR = '../miris_test_'
BASE_VIDEO_DIR = '../miris-data/'

#SPLITTERS = {'track': TrackSplitter()}
SPLITTERS = {'track': TrackSplitter(), 'area7': AreaSplitter(0.7), 'area3': AreaSplitter(0.3), 'area1': AreaSplitter(0.1), 'area7': AreaSplitter(0), 
            'tarea7': AreaTrackSplitter(0.7), 'tarea3': AreaTrackSplitter(0.3), 'tarea1': AreaTrackSplitter(0.1), 'tarea7': AreaTrackSplitter(0)}

# need to fix get queries
def main(base_dir, storage_dir):
    logging.basicConfig(filename="miris_get.log", level=logging.INFO)
    managers = {}
    for splitter in SPLITTERS:
        dire = storage_dir + splitter
        managers[splitter] = FullStorageManager(miris_tagger, SPLITTERS[splitter], dire)
    
    for query in TEST_QUERIES:
        logging.info('~~~~~~~~~~~~~~~~NEW QUERY~~~~~~~~~~~~~~~~~')
        logging.info(query)
        for splitter in managers:
            logging.info('!!!!!!!!!!!!!!!!!!!!NEW MANAGER!!!!!!!!!!!!!!!!!!!!')
            logging.info(splitter)
            manager = managers[splitter]
            for video in VID_NAMES:
                query = query.format(video)
                logging.info(query)
                logging.info(video)
                start_query = time.time()
                
                total_pixels = 0.0
                clips = manager.get(query)
                print(len(clips))
                for clip in clips:
                    if clip[0] != None and clip[1] != None and clip[2] != None:
                        total_pixels += clip[1]*clip[2]/10000.0

                end_query = time.time()
                logging.info("query time: " + str(end_query - start_query))
                logging.info("total pixels: " + str(total_pixels))

if __name__ == '__main__':
    main(BASE_VIDEO_DIR, BASE_STORAGE_DIR)