"""This file is part of DeepLens which is released under MIT License and
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered_videoio.py uses opencv (cv2) to read and write files to disk. It contains
primitives to encode and decode archived and regular video formats for a tiered
storage system.
"""
from deeplens.struct import Box
import cv2
from deeplens.tracking.contour import *

class SizeMovementTagger(KeyPoints):
    """Categorizes Moving Objects By Size
    """

    def __init__(self, \
                 blur=5, \
                 bilat=150,
                 edge_low=40, \
                 edge_high=50, \
                 area_thresh=500): #min size
        """The constructor takes in some parameters for the detector.
        """

        self.blur = blur
        self.edge_low = edge_low
        self.edge_high = edge_high
        self.area_thresh = area_thresh
        self.bilat = bilat
        self.scale = 1.0

    def __iter__(self):
        self.prev = None
        return super().__iter__()

    """the map function creates bounding boxes of the form x,y,x,y to identify detection points
    """
    def map(self, data):
        ff = data

        if len(ff['data'].shape) < 3:
            gray = ff['data']
        else:
            gray = ff['data'][:,:,0]


        blurred = cv2.GaussianBlur(gray, (self.blur, self.blur), 0)
        tight = cv2.Canny(blurred, self.edge_low, self.edge_high)

        if not (self.prev is None):
            mask = np.abs((self.prev - tight) > 0).astype(np.uint8)
            self.prev = tight

            img = tight*mask
            tight = cv2.bilateralFilter(img,7,self.bilat,self.bilat)
        else:
            self.prev = tight


        contours, _ = cv2.findContours(tight.copy(), cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        
        rtn = []
        for cnt in contours:
            points = cv2.boundingRect(cnt)
            print(points)
            box = Box(points[0],points[1],points[0] + points[2],points[1] + points[3])
            if cv2.contourArea(cnt) > self.area_thresh:
                M = cv2.moments(cnt)
                #cx = int(M['m10']/M['m00'])
                #cy = int(M['m01']/M['m00'])

                rtn.append({'label': 'large','bb': box})
            elif cv2.contourArea(cnt) > 25:
                M = cv2.moments(cnt)
               #cx = int(M['m10']/M['m00'])
                #cy = int(M['m01']/M['m00'])

                rtn.append({'label': 'small','bb': box})

        #print(len(rtn))
        ff['objects'] = rtn

        return ff


    def _serialize(self):
        return {'blur': self.blur,
                'edge_low': self.edge_low,
                'edge_high': self.edge_high,
                'area_thresh': self.area_thresh,
                'label': self.label}