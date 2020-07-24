import cv2
import time
import matplotlib.pyplot as plt
import os
import json
import shutil
from deeplens.utils.utils import * 
from deeplens.streams import CVVideoStream
from deeplens.utils.constants import *
from deeplens.utils.testing_utils import *
from deeplens.full_manager import *
from test_miris import *

def test1():
    cap = cv2.VideoCapture('./videos/BigBuckBunny.mp4')

    batch_sizes = [i for i in range(101)]
    #batch_sizes = [100]
    #fourcc = int(cap.get(cv2.CAP_PROP_FOURCC))
    #print(fourcc)
    fourcc = cv2.VideoWriter_fourcc(*'FMP4')
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    folder_name = []

    font = cv2.FONT_HERSHEY_SIMPLEX
    count = 0
    #writer = cv2.VideoWriter('./videos/cut_copy.mkv', fourcc, 1, (width, height))


    # test batch sizes
    test_times = []
    batch_writers = {}

    for i in batch_sizes:
        name = './batch_bunny/batch' + str(i)
        if os.path.exists(name):
            shutil.rmtree(name)
        os.mkdir(name)

    name = './batch_bunny/batch0'
    if os.path.exists(name):
        shutil.rmtree(name)
    os.mkdir(name)
    dire = './batch_bunny/batch0' 
    name = get_rnd_strng()
    name = add_ext(name, '.avi')
    name = os.path.join(dire, name)
    batch_writers[0] = cv2.VideoWriter(name, fourcc, 1, (width, height))
    count = 0
    for i in range(5000):
        ret, frame = cap.read()
        if not ret:
            break
        for i in batch_sizes:
            if i == 0:
                continue
            if count%i == 0:
                dire = './batch_bunny/batch' + str(i)
                name = get_rnd_strng()
                name = add_ext(name, '.avi')
                name = os.path.join(dire, name)
                batch_writers[i] = cv2.VideoWriter(name, fourcc, 1, (width, height))
            batch_writers[i].write(frame)
        batch_writers[0].write(frame)
        count += 1

    print('finished writing')
    results = []
    for i in batch_sizes:
        name = './batch_bunny/batch' + str(i)
        results.append(get_size(name))

    f = open('./batch_bunny/batch.json', 'w')
    json.dump(results, f)
    f.close()

    plt.plot(batch_sizes, results)
    plt.show()

def test2():
    results = []
    base = './batch_bunny/'
    for dire in os.listdir(base):
        p = os.path.join(base, dire)
        if os.path.isdir(p):
            files = []
            for f in os.listdir(p):
                if f.endswith('.avi'):
                    f = os.path.join(p, f)
                    files.append(f)
            new_base = os.path.join('./batch_bunny_2', dire)
            os.mkdir(new_base)
            convertFormat(files, new_base)
            results.append(get_size(new_base))
    f = open('./batch_bunny_2/batch.json', 'w')
    json.dump(results, f)
    f.close()

def test3():
    base = './batch_bunny_2/'
    results = []
    for i in range(101):
        batch = 'batch' + str(i)
        p = os.path.join(base, batch)
        results.append(get_size(p))
    f = open('./batch_bunny_2/batch.json', 'w')
    json.dump(results, f)
    f.close()

# Test storage miris
def test4():
    cap = cv2.VideoCapture('./videos/shibuya.mp4')
    #batch_sizes = [30]
    #batch_sizes = [100]
    #fourcc = int(cap.get(cv2.CAP_PROP_FOURCC))
    #print(fourcc)
    batch_sizes = [i for i in range(101)]
    fourcc = cv2.VideoWriter_fourcc(*'FMP4')
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    folder_name = []

    count = 0
    #writer = cv2.VideoWriter('./videos/cut_copy.mkv', fourcc, 1, (width, height))


    # test batch sizes
    test_times = []
    batch_writers = {}

    for i in batch_sizes:
        name = './batch_shibuya/batch' + str(i)
        if os.path.exists(name):
            shutil.rmtree(name)
        os.mkdir(name)

    name = './batch_shibuya/batch0'
    if os.path.exists(name):
        shutil.rmtree(name)
    os.mkdir(name)
    dire = './batch_shibuya/batch0' 
    name = get_rnd_strng()
    name = add_ext(name, '.avi')
    name = os.path.join(dire, name)
    batch_writers[0] = cv2.VideoWriter(name, fourcc, 1, (width, height))
    count = 0
    for i in range(5000):
        ret, frame = cap.read()
        if not ret:
            break
        for i in batch_sizes:
            if i == 0:
                continue
            if count%i == 0:
                dire = './batch_shibuya/batch' + str(i)
                name = get_rnd_strng()
                name = add_ext(name, '.avi')
                name = os.path.join(dire, name)
                batch_writers[i] = cv2.VideoWriter(name, fourcc, 1, (width, height))
            batch_writers[i].write(frame)
        batch_writers[0].write(frame)
        count += 1

    print('finished writing')
    results = []
    for i in batch_sizes:
        name = './batch_shibuya/batch' + str(i)
        results.append(get_size(name))

    f = open('./batch_shibuya/batch.json', 'w')
    json.dump(results, f)
    f.close()

    plt.plot(batch_sizes, results)
    plt.show()

def test5(base_dir = '../experiments/data'):  
    vid_dirs = ['beach', 'shibuya', 'uav', 'warsaw']
    for d in vid_dirs:
        dire = os.path.join(base_dir, d)
        videos = os.path.join(dire, 'videos')
        for vid in os.listdir(videos):
            if vid.endswith('mp4'):
                name = vid.split('.')[0]
                vid_dir = os.path.join(videos, vid)
                cap = cv2.VideoCapture(vid_dir)

                width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
                height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
                fps = cap.get(cv2.CAP_PROP_FPS)
                fourcc = cv2.VideoWriter_fourcc(*'FMP4')

                name = d + name + '.avi'
                new_file = os.path.join(videos, name)

                writer = cv2.VideoWriter(new_file, fourcc, fps, (width, height))
                print(name)
                while True:
                    ret, frame = cap.read()
                    if not ret:
                        break
                    writer.write(frame)

                print(os.path.getsize(new_file))
# test get? -> is it worth testing get?


test5()