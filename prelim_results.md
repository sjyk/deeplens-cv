# Preliminary results

Video file: `https://www.dropbox.com/s/91t6ucfsq9u9vca/tcam.mp4?dl=0`

Test code:  
```python
pipeline = v[Crop(100, 450, 450, 950)][
    TensorFlowObjectDetect(model_file='ssd_mobilenet_v1_coco_2017_11_17',
                           label_file='mscoco_label_map.pbtxt',
                           num_classes=90, confidence=0.2)]
```

## Single python script running, entire video of 5m06s  
**Experiment 1.1**  
buffer_size = 100  
n_frames = 9183    
time: 616.77s  
Avg. CPU Usage: 42%  

**Experiment 1.2**  
buffer_size = 300  
n_frames = 9183  
time: 403.08s  
Avg. CPU Usage: 57%  

**Experiment 1.3**  
buffer_size = 1000  
n_frames = 9183  
time: 417.36s  
Avg. CPU Usage: 60%  

**Experiment 1.4**  
buffer_size = auto  
n_frames = 9183  
time: 384.83s  
Avg. CPU Usage: 62%  

## 6 python scripts simultaneously, 10 video chunks of 30s each  
**Experiment 2.1**  
Splitting the video costs ~600s (nearly at 1x speed)  
buffer_size = 800  
n_frames = 9000 in total, 900 each  
time: ~370s  
Avg CPU Usage: 91%  

**Experiment 2.2**  
6 python scripts simultaneously, 10 video chunks of 30s each  
buffer_size = 1000 (>900)  
n_frames = 9000 in total, 900 each  
time: ~390s  
Avg. CPU Usage: 90%  

**Experiment 2.3**  
6 python scripts simultaneously, 10 video chunks of 30s each  
buffer_size = 300  
n_frames = 9000 in total, 900 each  
time: ~320s  
Avg. CPU Usage: 95%  

**Experiment 2.4**  
6 python scripts simultaneously, 10 video chunks of 30s each  
buffer_size = 100  
n_frames = 9000 in total, 900 each  
time: ~330s  
Avg. CPU Usage: 87% (last 4 chunks doesn't fully utilize 6-core CPU)  

