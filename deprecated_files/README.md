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
We re-evaluate video storage, encoding, and compression with downstream query processing in mind. In particular, we find interesting analogies to data skipping and columnar storage. Videos can be temporally and spatially partitioned to isolate segments that are likely to contain certain types of objects. For example, segments of the video that do not contain cars at all can be safely skipped if we are interested in identifying red cars. These partitioned segments are further more compressible and faster to decode during query processing. 

## Basic API
The module provides a number of "storage managers" which offer an interface to store and retrieve video.
```
>>> manger.put('HappyPenguin.mp4', 'penguin')
>>> manger.put(DEFAULT_CAMERA, 'penguin')
>>> manger.put('http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4', 'penguin')
```
The module then allows users to quickly retrieve clips of a certain size that satisfy a target predicate:
```
>>> manger.get('penguin', hasLabel('penguin') ,5*DEFAULT_FRAME_RATE)
>>> manger.get('penguin', startsBefore(30) ,5*DEFAULT_FRAME_RATE)
```

## Installation
See `Install.md` for details.

