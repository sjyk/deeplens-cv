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

def getManager(manager, base_dir):
    
    vid_dirs = VID_TYPES
    names = []
    for d in vid_dirs:
        dire = os.path.join(base_dir, d)
        labels = os.path.join(dire, 'json')
        videos = os.path.join(dire, 'videos')
        for vid in os.listdir(videos):
            if vid.endswith('mp4'):
                name = vid.split('.')[0]
                lb_dir= os.path.join(labels, name + '-baseline.json')
                vid_dir = os.path.join(videos, vid)
                name = d + name
                print("~~NEW VIDEO~~", flush=True)
                print("name: " + name)
                print("original size: " + str(os.path.getsize(vid_dir)))
                names.append(name)
                lbs = {'tracking': JSONListStream(lb_dir, 'tracking', is_file = True, is_list = True)}
                start = time.time()
                manager.put(vid_dir, name, map_streams=lbs) # we might have to use put-many?
                end = time.time()
                print("storage size: " + str(manager.size(name)))
                print("put time: " + str(end - start), flush=True)


def main(base_dir, storage_dir):
    for splitter in SPLITTERS:
        print('!!!!!!!!!!!!!!!!!!!!NEW SPLITTER!!!!!!!!!!!!!!!!!!!!')
        print(splitter)
        dire = storage_dir + splitter
        manager = FullStorageManager(miris_tagger, SPLITTERS[splitter], dire)
        names = getManager(manager, base_dir)

if __name__ == '__main__':
    main(BASE_VIDEO_DIR, BASE_STORAGE_DIR)