from dlstorage.filesystem.videoio import *
from dlstorage.constants import *
from dlstorage.stream import *
from dlstorage.xform import *
from dlstorage.header import *
from dlstorage.utils.clip import *
from dlstorage.utils.debug import *


v = VideoStream(0, 300)
v = v[TestTagger()]

#write_video_clips(v, 'bear', 'Y800', ObjectHeader(), 10)
print(timeof(read_if('bear', AND(startsAfter(0), endsBefore(60)), 30)))


