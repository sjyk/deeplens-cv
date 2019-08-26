from dlstorage.filesystem.videoio import *
from dlstorage.constants import *
from dlstorage.stream import *
from dlstorage.xform import *
from dlstorage.header import *

v = VideoStream(0, 100)
v = v[TestTagger()]

write_video_clips(v, 'bear', MP4V, ObjectHeader(), 50)
print(diag_timeof(read_if('bear', TRUE)))
print(diag_sizeof('bear'))
