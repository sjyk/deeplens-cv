import numpy as np

def _array_shape(vstream):
    #gets the array shape of the vstream

    cnt = 0
    for frame in vstream:
        cnt += 1
        width, height, channels = frame['data'].shape

    return (cnt, width, height, channels)


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

