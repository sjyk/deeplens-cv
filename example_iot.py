from deeplens.struct import *
from deeplens.utils.ui import play
from deeplens.simple_manager.manager import SimpleStorageManager


CAMERA_ID = 0

#create vstream object
vstream = VideoStream(CAMERA_ID)
#play(vstream)

s = SimpleStorageManager('videos')
s.put(vstream, 'capture')





