import cv2
import numpy as np
import csv
from deeplens.struct import Box

"""
This file contains tagging function used to tag youtube videos with their provided bounding box
information
"""

def youtubeTagger(path, output):
    fullpath = path + output
    fh = open(fullpath, 'r')
    csvreader = csv.reader(fh)
    outputDict = dict()
    for i,row in enumerate(csvreader):
        if (i== 0):

            for j,s in enumerate(row):
                if j == 0:
                    lstOfFrameInfos = outputDict.get(s)
                else:
                    lstOfFrameInfos.append(s)
