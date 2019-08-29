from dlstorage.filesystem.videoio import *
from dlstorage.constants import *
from dlstorage.stream import *
from dlstorage.xform import *
from dlstorage.header import *
from dlstorage.utils.clip import *
from dlstorage.utils.debug import *


v = VideoStream(0, 10)
v = v[TestTagger()]

write_video_clips(v, 'bear', MP4V, ObjectHeader(), 5)
#print(timeof())
#print(sizeof('bear'))

materialize_clip((0,7), [(0,5),(6,10)] ,read_if('bear', TRUE))


