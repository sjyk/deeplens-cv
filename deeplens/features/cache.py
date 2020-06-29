import numpy as np
from deeplens.struct import RawVideoStream

#does not currently handle meta data
def persist(vstream, file):
    materialized_stream = [frame['data'] for frame in vstream]
    np.savez(file, *materialized_stream)


#does not currently handle meta, returns a ref to the meta data
def materialize(vstream):
    materialized_stream = [frame['data'] for frame in vstream]
    return RawVideoStream(materialized_stream)

def load(materialized_stream):
    npzfile = np.load(file)
    materialized_stream = [npzfile[arr] for arr in npzfile.files]
    return RawVideoStream(materialized_stream)
