"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered_file.py contains basic functions for dealing with .ref directories.

.ref directories contain a reference text file which states the directory
of the video file."""

import pickle
import string
import os 
import tarfile
import random
import cv2
from dlstorage.filesystem.file import *

#import all of the constants
from dlstorage.constants import *

def is_ref_name(name):
    """Check if a given name is a reference directory
    """
    ext_name = name.split('/')[-1].split('.')[1]
    if ext_name == 'ref':
        return True
    else:
        return False

def write_ref_file(ref_file, file_name):
    """Args:
        ref_file (string) - reference file
        file_name (string) - name of external file
    """
    f = open(ref_file, "w")
    f.write(file_name)
    f.close()

def read_ref_file(ref_file):
    """ get external directory from reference file
    """
    with open(ref_file, "r") as f:
        ref = f.readline()
        return ref


def delete_ref_file(ref_file):
    """ delete reference file
    """
    if os.path.exists(ref_file):
        os.remove(ref_file)
        return True
    else:
        return False