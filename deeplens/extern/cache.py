import numpy as np
import os

def _array_shape(vstream):
    #gets the array shape of the vstream

    cnt = 0
    for frame in vstream:
        cnt += 1
        width, height, channels = frame['data'].shape

    return (cnt, width, height, channels)

def is_cache_file(filename):
    root, ext = os.path.splitext(filename)
    return (ext == '.npz')

def videoref_2_cache(filename):
    if is_cache_file(filename):
        raise ValueError(filename + ' is already a cache file')

    root, ext = os.path.splitext(filename)
    return root + '.npz'

def cache_2_videoref(filename):
    if not is_cache_file(filename):
        raise ValueError(filename + ' is already a video file')

    root, ext = os.path.splitext(filename)
    return root + '.avi'

def delete_cache(file):
    os.remove(file)

#does not currently handle meta data
def persist(vstream, file):
    shape = _array_shape(vstream)

    fp = np.memmap(file, dtype='uint8', mode='w+', shape=shape)

    for i,frame in enumerate(vstream):
        fp[i,:,:,:] = frame['data']

    del fp

    return shape

        
#does not currently handle meta, returns a ref to the meta data
def materialize(vstream):
    materialized_stream = [frame['data'] for frame in vstream]
    return RawVideoStream(materialized_stream)

