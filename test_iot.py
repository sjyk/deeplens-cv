from deeplens.struct import *

from deeplens.tracking.contour import *
from deeplens.tracking.event import *

from deeplens.utils.ui import play, overlay

#VIDEO File Name
#FILE_NAME = 'iot_videos/khnl6v2j10017mv_1582586053225_fridge_idle1.mp4'

#FILE_NAME = 'iot_videos/khnl6v2j10017mv_1582586287285_fridge_door5.mp4'
FRAME_RATE = 24

#create vstream object
vstream = VideoStream(FILE_NAME)

region = Box(175,90,250,230) #region where the fridge is

fridge_session_length_s = 30 #minimum time between events.

vstream = vstream[KeyPoints()]\
	             [ActivityMetric('door', region)]\
	             [Filter('door', [1], 0, delay=FRAME_RATE*fridge_session_length_s)]

for v in vstream:
	if v['door']:
		print('Door Opened At', int(v['frame']/FRAME_RATE), '(s)')



