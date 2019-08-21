# deeplens-cv
We design an adaptive dataflow system for processing video streams with geometric and learning-based computer vision primitives.

## Stream API
A video stream is an iterator over frames and can be constructed as follows:
```
>>> from dlcv.struct import *
>>> v = VideoStream('tcam.mp4')
```
This code opens up an iterator over frames in the tcam.mp4 file which can be downloaded from
```
https://www.dropbox.com/s/91t6ucfsq9u9vca/tcam.mp4?dl=0
```
If you want to pull a limited number of frames you can set a limit, let's say 100:
```
>>> v = VideoStream('tcam.mp4', limit=100)
```
URLs can similarly be directly used in the streaming system:
```
>>> v = VideoStream('https://www.dropbox.com/s/91t6ucfsq9u9vca/tcam.mp4?dl=0')
```
So can cameras:
```
>>> v = VideoStream(DEFAULT_CAMERA)
```

## Transformation API
dlcv provides a compositional API for transforming streams of video. For example, 
```
>>> from dlcv.dataflow.map import *
>>> v[Crop(0,0,200,200)] 
```
These operations can be composed:
```
>>> from dlcv.dataflow.map import *
>>> v[Crop(0,0,200,200)][Sample(0.5)]
```
You can see the effects of transformations using the utils functions:
```
>>>from dlcv.utils import *
>>>play(v[Crop(0,0,200,200)][Sample(0.5)])
```

## Detection API
In dlcv there are Metrics and Events. Metrics translate geometric
vision primtives into numerical time-series, and Events detect patterns
in these time-series. Test.py shows several examples of thes pipelines.



# deeplens-storage
Storage Manager For The DeepLens System
