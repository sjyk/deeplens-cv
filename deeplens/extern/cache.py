import numpy as np
from deeplens.struct import RawVideoStream

#does not currently handle meta data
def persist(vstream, file):
    materialized_stream = [frame['data'] for frame in vstream]
    np.savez(file, *materialized_stream)

def load(file):
    npzfile = np.load(file)
    materialized_stream = [npzfile[arr] for arr in npzfile.files]
    return RawVideoStream(materialized_stream)
