
import os
import sqlite3
import logging
import time
import random
import json

TEST_QUERIES = "SELECT DISTINCT label.clip_id, fwidth, fheight, frame FROM label LEFT JOIN clip ON label.clip_id = clip.clip_id WHERE label.video_name='{}' AND label='{}' ORDER BY frame "
# type of videos used from the miris dataset
VID_NAMES = ['beach0', 'beach1', 'beach2', 'beach3', 'beach4', 'shibuya0', 'shibuya1', 'shibuya2', 'shibuya3', 'shibuya4', 
'uav0006', 'uav0007', 'uav0008', 'uav0009', 'uav0011', 'warsaw0', 'warsaw1', 'warsaw2', 'warsaw3', 'warsaw4']

#['beach0', 'beach1', 'beach2', 'beach3', 'beach4', 'beach5', 'shibuya0', 'shibuya1', 'shibuya2', 'shibuya3', 'shibuya4', 'shibuya5', 'uav0006', 'uav0007', 'uav0008', 'uav0009', 'uav0011', 'warsaw0', 'warsaw1', 'warsaw2', 'warsaw3', 'warsaw4', 'warsaw5']
#directory of the storage manager
BASE_STORAGE_DIR = '../miris_test_'
label = [717, 333, 319, 649, 582, 356, 88, 762, 909, 5, 956, 817, 233, 854, 985, 781, 937, 289, 686, 441]
#label = [random.randint(0, 999) for i in range(len(VID_NAMES)) ]
print(label)
#label = [162, 254, 429, 450, 837, 799, 329, 738, 691, 197, 995, 914, 51, 385, 720, 375, 132, 965, 867, 748]
#SPLITTERS = {'track': TrackSplitter()}
SPLITTERS = ['track', 'area0','area1', 'area3','area7']
#SPLITTERS = ['track']
frame_size = 51.84

def main():
    logging.basicConfig(filename="miris_get_track_final.log", level=logging.INFO)
    dirs = []
    for splitter in SPLITTERS:
        dirs.append(BASE_STORAGE_DIR + splitter)
    query = TEST_QUERIES
    logging.info('~~~~~~~~~~~~~~~~NEW QUERY~~~~~~~~~~~~~~~~~')
    logging.info(query)
    for dire in dirs:
        logging.info('!!!!!!!!!!!!!!!!!!!!NEW MANAGER!!!!!!!!!!!!!!!!!!!!')
        logging.info(dire)
        conn = sqlite3.connect(os.path.join(dire, 'header.db'))
        total_pixels = {}
        frame_num = {}
        block_num = {}
        bbox_pixels = {}
        
        for video in VID_NAMES:
            logging.info(video)
            for lb in label:
                q = query.format(video, str(lb))
                logging.info(lb)
                video_name = ''.join([i for i in video if not i.isdigit()]) 
                if video_name not in total_pixels:
                    total_pixels[video_name] = 0.0
                    frame_num[video_name] = 0
                    block_num[video_name] = 0

                c = conn.cursor()
                c.execute(q)
                results = c.fetchall()
                curr_frame = 0
                cache = []
                # for frame in results:
                #     if frame[0] != None and frame[1] != None and frame[2] != None:
                #         if curr_frame != frame[3]:
                #             frame_num[video_name] += 1
                #             if len(cache) != 0:
                #                 total_pixels[video_name] += min(cache)
                #             cache = []
                #             curr_frame = frame[3]
                #         cache.append(frame[1]*frame[2]/100.0) 
                # total_pixels[video_name] += min(cache)
                ids = set([results[i][0] for i in range(len(results))])
                block_num[video_name] += len(ids)
        
        #logging.info(total_pixels)
        #logging.info(frame_num)
        logging.info(block_num)

if __name__ == '__main__':
    main()
    