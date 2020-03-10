import pandas as pd
import subprocess
import cv2
import os
import time
from deeplens.media.youtube import *

#download the youtube-bb Object Detection in Video Segments - training set file and copy to path folder
input_file="yt_bb_detection_train.csv"
path="./train/"

#extract the video according to detected object's timestamp
def extract_video(video,start_time,duration,path="./"):
    try:
        video_name=path+video+".mp4"
        tmp=path+"tmp_trim.mp4"
        if os.path.exists(video_name):
            os.remove(video_name)

        youtube_fetch(video, path)
        command = "ffmpeg -ss "+start_time+" -t "+duration+" -y -i "+video_name +"  "+tmp
        subprocess.call(command, shell=True)
        os.remove(video_name)
        os.rename(tmp,video_name)
    except Exception as e:
        print(e)

#extract_video('AAB6lO-XiKE', "10","20","hello1123")
def extract_props_toCSV(path, input_file):
    df = pd.read_csv(path+input_file, sep=',',
                 dtype={'youtube_id': str, 'timestamp_ms': int, 'class_name': str, 'xmin': float,'xmax':float,"ymin":float,"ymax":float})
    df = pd.DataFrame(df)
    youtube_ids=df['youtube_id']
    youtube_ids2=list(dict.fromkeys(youtube_ids))
    
    trim_id=[]
    trim_frame_no=[]
    trim_class=[]
    trim_xmin=[]
    trim_xmax=[]
    trim_ymin=[]
    trim_ymax=[]
    trim_fps=[]
    trim_seconds=[]
    missing_files=[]
    
    for item in youtube_ids2:
        print("working on file:",item)
        id=df[df['youtube_id']==item]
        time_start=min(id['timestamp_ms'])/1000
        time_end=max(id['timestamp_ms'])/1000
        duration=time_end-time_start
        try:
            extract_video(item, str(time_start), str(duration),path)
        except:
            missing_files.append(item)
            print("warning!!:",item)
            data = {"missing file": missing_files}
            df_data = pd.DataFrame(data)
            df_data.to_csv(path + "missingfile.csv", index=False)
            continue
        video_file=path+item+".mp4"
        time.sleep(1)
        if os.path.exists(video_file):
            cap = cv2.VideoCapture(item+".mp4")
            fps = round(cap.get(cv2.CAP_PROP_FPS))
            cap.release()
            if fps==0:
                print("warning!missing fps:",item,",set default fps=30")
                fps=30
            else:
                print("missing file:"+video_file)
                for index, row in id.iterrows():
                    trim_id.append(row['youtube_id'])
                    trim_seconds.append(row['timestamp_ms']/1000-time_start)
                    trim_frame_no.append(int((row['timestamp_ms']/1000-time_start)*fps))
                    trim_class.append(row['class_name'])
                    trim_xmin.append(row['xmin'])
                    trim_xmax.append(row['xmax'])
                    trim_ymin.append(row['ymin'])
                    trim_ymax.append(row['ymax'])
                    trim_fps.append(fps)

        print(item+"duration:"+str(duration))
        data = {"youtube_id": trim_id, "second_no": trim_seconds, "fps": trim_fps, "frame_no": trim_frame_no,
            "class": trim_class, "xmin": trim_xmin, "xmax": trim_xmax, "ymin": trim_ymin, "ymax": trim_ymax}
        df_out = pd.DataFrame(data)
        df_out.to_csv(path + "processed_" + input_file, index=False)


    print("done!")

extract_props_toCSV(path, input_file)