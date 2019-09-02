from dlstorage.filesystem.videoio import *
from dlstorage.constants import *
from dlstorage.stream import *
from dlstorage.xform import *
from dlstorage.header import *
from dlstorage.utils.clip import *
from dlstorage.utils.debug import *


v = VideoStream('/Users/sanjaykrishnan/Downloads/BigBuckBunny.mp4', 1000)
v = v[TestTagger()]

delete_video("bear")
print(timeof(write_video_clips(v, 'bear', 'Y800', ObjectHeader(), 100)))
print(timeof(read_if('bear', startsBefore(300) , 250)))


