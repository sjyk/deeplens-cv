"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

constants.py defines encoding and numerical constants.
"""

#compression constants
GZ,BZ2,RAW = 'w:gz','w:bz2','w'

#encoding constants
XVID, DIVX, H264, MP4V, UNENC, GSC = 'XVID', 'DIVX', 'X264', 'FMP4', 'MJPG', 'Y800'

#default file out
AVI = '.avi'

#default frame rate
DEFAULT_FRAME_RATE = 30.0

#temp files
DEFAULT_TEMP = '/tmp'