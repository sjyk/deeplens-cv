"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

condition.py defines a implemented Condition class and related functions

"""

from deeplens.core import *

#implements filter and crop only
class SimpleCondition(Condition):

	def __init__(self,filter=TRUE, crop=None):
		self.filter = filter
		self.crop = crop
        super().__init__(filter, crop)

    def checkHeader(self, header):
        #TODO: write checkHeader