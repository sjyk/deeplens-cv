from deeplens.struct import *

from deeplens.tracking.contour import *
from deeplens.tracking.event import *
from deeplens.dataflow.agg import get

from deeplens.utils.ui import play, overlay

#VIDEO File Name
FILE_NAME = 'iot_videos/khnl6v2j10017mv_1582586053225_fridge_idle1.mp4'
#FILE_NAME = 'iot_videos/khnl6v2j10017mv_1582586199865_fridge_door3.mp4'

FRAME_RATE = 24

#create vstream object
vstream = VideoStream(FILE_NAME)

region = Box(200,90,250,110) #region where the fridge is

fridge_session_length_s = 30 #minimum time between events.

vstream = vstream[KeyPoints(blur=1)]\
	             [ActivityMetric('door', region)]\
	             [Filter('door', [1], 0, delay=FRAME_RATE*fridge_session_length_s)]

for time, image in get(vstream, 'door', FRAME_RATE):
	print('Door Opened at', time, 's')

	#hit q to exit
	cv2.imshow('Player',image)
	if cv2.waitKey(0) & 0xFF == ord('q'):
		exit()




