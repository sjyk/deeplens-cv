from deeplens.utils.box import Box
import os
from deeplens.streams import *
import logging

VID_TYPES = ['beach', 'shibuya', 'uav', 'warsaw']
BASE_VIDEO_DIR = '../experiments/data'

def miris_optimal(stream, name):
    #total_area = 0.0
    frame_num = 0.0
    while True:
        try:
            curr = stream.next(name)
        except:
            return frame_num
        if curr is not None:
            add = False
            for lb in curr:
                bb = Box(lb['left']//2, lb['top']//2, lb['right']//2, lb['bottom']//2)
                if bb.x0 < 480 or bb.y0 < 270:
                    if not add:
                    frame_num += bb.area()/10000.0
    return frame_num


def putManager():
    
    vid_dirs = VID_TYPES
    areas = {}
    for d in vid_dirs:
        dire = os.path.join(BASE_VIDEO_DIR, d)
        labels = os.path.join(dire, 'json')
        videos = os.path.join(dire, 'videos')
        for vid in os.listdir(videos):
            if vid.endswith('mp4'):
                name = vid.split('.')[0]
                lb_dir= os.path.join(labels, name + '-baseline.json')
                lbs = JSONListStream(lb_dir, 'tracking', is_file = True, is_list = True).add_iter('test')
                area = miris_optimal(lbs, 'test')
                if d not in areas:
                    areas[d] = area
                else:
                    areas[d] += area
    print(areas)

if __name__ == '__main__':
    putManager()

