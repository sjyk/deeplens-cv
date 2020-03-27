
"""Test defines a number of test scenarios that are informative of how the API works
"""

from deeplens.utils.ui import *
from deeplens.media.segmentation import *
from deeplens.struct import *
from deeplens.media.youtube import *
from deeplens.media.matching import *

#filename = '/tmp/gbinYbEuL-M.mp4'
#youtube_fetch('gbinYbEuL-M')
youtube_fetch('otFsePa6MnI')
#exit()

#filename1 = '/tmp//q5BbWNXfw0s.mp4'
filename2 = '/tmp//otFsePa6MnI.mp4'

#files1 = shot_segmentation(filename1, threshold=75, skip=1)
files2 = shot_segmentation(filename2, threshold=75, skip=1)

#for seg1, file1 in files1:
#	for seg2, file2 in files2:
#		if is_video_match(file1, file2, thresh=55, sampling=0.01):
#			print(seg1, seg2, file1, file2, 'match')


#out = is_video_match('/tmp//otFsePa6MnI.mp4.10.avi', '/tmp//q5BbWNXfw0s.mp4.14.avi', sampling=0.01)
#print(out)

"""
cv2.imshow('Player',img)
if cv2.waitKey(0) & 0xFF == ord('q'):
	exit()
"""


#youtube_fetch('otFsePa6MnI')

#files = shot_segmentation(filename, threshold=50, skip=1)
#seg, file1 = files[0]
#seg, file2 = files[1]
#print(is_video_match(file1, file1))
#print(is_video_match(file1, file2))




