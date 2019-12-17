"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tests.py countains simple unit tests on tiered_manager.py
"""
import itertools

from dlstorage.tieredsystem.tiered_manager import * 
from dlstorage.utils.debug import *
from dlstorage.utils.ui import *
from dlstorage.storage_partition import storage_partition
VID_DIRECTORY = '../test_vid/roller_coaster.mp4'# directory of single testing video

STORAGE_DIRECTORY = '../test_store' # directory of storage system

EXTERNAL_DIRECTORY = '../test_extern' # directory of external storage location


def test_f(header):
    if header['seq'] == 0:
        return True
    else:
        return False
def main():
    manager = TieredStorageManager(TestTagger(), TestSplitter(), STORAGE_DIRECTORY, EXTERNAL_DIRECTORY)
    manager.put(VID_DIRECTORY, 'test')
    
    #manager.put(VID_DIRECTORY, 'test_e', in_extern_storage = True)
    
    #list_of_clips = manager.get('test_e', lambda x: True, 5)
    #list_of_clips = manager.get('test', lambda x: True, 5)
    #list_of_clips = manager.get('test', test_f, 5)
    #manager.moveToExtern('test_e', lambda x: True)
    #storage_partition(manager)
    #play(itertools.chain(*list_of_clips))
    # do delete with internal + external
    #manager.delete('test')
    #manager.delete('test_e')
    
    # # do size with internal + external
    #print(manager.size('test'))
    #print(manager.size('test_e'))

    #manager.moveToExtern('test', lambda x: True)
    #manager.moveToExtern('test_e', lambda x: True)


    #manager.moveFromExtern('test', lambda x : True)
    #manager.moveFromExtern('test_e', lambda x : True)

    #print(manager.isExtern('test', lambda x : True))
    #print(manager.isExtern('test_e', lambda x : True))

    

if __name__ == '__main__':
    main()