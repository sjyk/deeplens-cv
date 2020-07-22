from test_miris import *
from deeplens.utils.testing_utils import *
import os
import time

# get queries -> requires to return a list of clip_ids 
#TEST_QUERIES = {"SELECT * FROM clip WERE is_background = 0"}
TEST_QUERIES = {}

# type of videos used from the miris dataset
VID_TYPES = ['beach', 'shibuya', 'uav', 'warsaw']
#VID_TYPES = ['shibuya']

#directory of the storage manager
STORAGE_DIR = 'miris_h264'

def putManager(manager, base_dir, splitter):
    
    vid_dirs = VID_TYPES
    names = []
    
    for d in vid_dirs:
        dire = os.path.join(base_dir, d)
        labels = os.path.join(dire, 'json')
        videos = os.path.join(dire, 'videos')
        
        for vid in os.listdir():
            if vid.endswith('mp4'):
                name = vid.split('.')[0]
                lb_dir= os.path.join(labels, name + '-baseline.json')
                vid_dir = os.path.join(videos, vid)
                name = d + name
                print("~~NEW VIDEO~~", flush=True)
                print("name: " + name)
                print("original size: " + str(os.path.getsize(vid_dir)))
                names.append(name)
                lb = {'tracking': JSONListStream(lb_dir, 'tracking', 'labels', isList = True)}
                start = time.time()
                manager.put(vid_dir, name, aux_streams = lb) # we might have to use put-many?
                end = time.time()
                print("storage size: " + str(manager.size(name)))
                print("put time: " + str(end - start), flush=True)


def main(base_dir, splitter, storage_dir, put = True):
    manager = FullStorageManager(miris_tagger, splitter, storage_dir)
    if put:
        names = putManager(manager, base_dir, splitter)
    # get
    queries = TEST_QUERIES
    for query in queries:
        print('~~' + query + '~~')
        start_query = time.time()
        vstreams, clip_ids = manager.get_vstreams(queries[query])
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
    if len(sys.argv) < 3:
        print("Enter dataset basepath as argv[1], crop number/splitter as argv[2]")
        exit(1)
    
    base_dir = sys.argv[1]
    crop = sys.argv[2]
    if crop.isdigit():
        crop_num = int(sys.argv[2])
        splitter = CropAreaSplitter(crop_num)
    elif crop == 'track':
        splitter = TrackSplitter()
    elif crop == 'area':
        splitter = AreaSplitter()
    else:
        print("invalid value for splitter")
        exit(1)
    storage = STORAGE_DIR + sys.argv[2]
    main(base_dir, splitter, storage)