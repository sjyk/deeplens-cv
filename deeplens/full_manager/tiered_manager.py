"""This file is part of DeepLens which is released under MIT License and 
is copyrighted by the University of Chicago. This project is developed by
the database group (chidata).

tiered.py defines file storage operations. These operations allows for
complex file storage systems such as tiered storage, and easy file
movement and access.

""" 


#TODO: Note, we ignored multi-threading for now
from deeplens.simple_manager.tiered import *
from deeplens.simple_manager.videoio import *
from deeplens.full_manager.tiered_videoio import *
from deeplens.full_manager.tiered_file import *

from deeplens.constants import *
from deeplens.struct import *
from deeplens.dataflow.map import Sample
from deeplens.header import *
from deeplens.error import *

import os

DEFAULT_ARGS = {'encoding': MP4V, 'size': -1, 'limit': -1, 'sample': 1.0, 'offset': 0, 'auto_split': True}

# NOTE: bounding boxes are at a clip level

class TieredStorageManager(StorageManager):
    """ TieredStorageManager is the implementation of a 3 tiered
    storage manager, with a cache and external storage, where cache
    is in the same location as disk, and external storage is another
    directory
    """
    def __init__(self, content_tagger, content_splitter, basedir, externdir):
        self.content_tagger = content_tagger
        self.content_splitter = content_splitter
        self.basedir = basedir
        self.externdir = externdir
        self.videos = set()
        self.threads = None
        self.STORAGE_BLOCK_SIZE = 60 

        if not os.path.exists(basedir):
            try:
                os.makedirs(basedir)
            except:
                raise ManagerIOError("Cannot create the directory: " + str(basedir))

        if not os.path.exists(externdir):
            try:
                os.makedirs(externdir)
            except:
                raise ManagerIOError("Cannot create the directory: " + str(externdir))

    def put(self, filename, target, args=DEFAULT_ARGS, in_extern_storage = False):
        """put adds a video to the storage manager from a file. It should either add
            the video to disk, or a reference in disk to deep storage.
        """
        v = VideoStream(filename, args['limit'])
        v = v[Sample(args['sample'])]
        v = v[self.content_tagger]
        
        physical_clip = os.path.join(self.basedir, target)
        delete_video_if_exists(physical_clip)
        if in_extern_storage: 
            physical_video = os.path.join(self.externdir, target)
        else:
            physical_video = None
        
        if args['size'] == -1:
            if args['auto_split']:
                v = v[self.content_splitter]
                write_video_auto(v, \
                        physical_clip, args['encoding'], \
                        ObjectHeader(offset=args['offset']),
                        output_extern = physical_video )
            else:
                write_video(v, \
                            physical_clip, args['encoding'], \
                            ObjectHeader(offset=args['offset']),
                            output_extern = physical_video )
        else:
            write_video_clips(v, \
                              physical_clip, \
                              args['encoding'], \
                              ObjectHeader(offset=args['offset']), \
                              args['size'],
                              output_extern = physical_video )
        
        self.videos.add(target)

    def get(self, name, condition, clip_size):
        """retrievies a clip of a certain size satisfying the condition.
        If the clip was in external storage, get moves it to disk.
        """
        if name not in self.videos:
            raise VideoNotFound(name + " not found in " + str(self.videos))

        physical_clip = os.path.join(self.basedir, name)
        return read_if(physical_clip, condition, clip_size) #TODO: update header here
    
    def delete(self, name):
        disk_files = os.path.join(self.basedir, name)
        
        if name in self.videos:
            self.videos.remove(name)
        
        delete_video_if_exists(disk_files)


    def list(self):
        return list(self.videos)
    
    #TODO implement after we support threading
    def setThreadPool(self):
        raise ValueError("This storage manager does not support threading")

    def size(self, name):
        """ Return the total amount of space a deeplens video takes up
        (including headers)
        """
        seq = 0
        size = 0
        physical_clip = os.path.join(self.basedir, name)

        file = add_ext(physical_clip, '.start') 
        size += os.path.getsize(file)
        while True:
            try:
                file = add_ext(physical_clip, '.seq', seq) 
                size += sum(os.path.getsize(os.path.join(file,f)) for f in os.listdir(file))
                seq += 1
                continue
            except FileNotFoundError:
                pass
            try:
                file = add_ext(physical_clip, '.ref', seq) 
                extern_dir = read_ref_file(file) # Count external space
                size += sum(os.path.getsize(os.path.join(extern_dir,f)) for f in os.listdir(extern_dir))
                seq += 1
                continue
            except FileNotFoundError:
                pass
            break
        return size
    
    def moveToExtern(self, name, condition): 
        """ Move clips that fulfil the condition to disk
        """
        extern_dir = os.path.join(self.externdir, name)
        physical_clip = os.path.join(self.basedir, name)
        return move_to_extern_if(physical_clip, condition, extern_dir)

    def moveFromExtern(self, name, condition): 
        """ Move clips that fulfil the condition to disk
        """
        physical_clip = os.path.join(self.basedir, name)
        move_from_extern_if(physical_clip, condition, threads=None)

    def isExtern(self, name, condition):
        """ Default: returns True if any of the files that meet the requirements
        is in external storage
        """
        physical_clip = os.path.join(self.basedir, name)
        return check_extern_if(physical_clip, condition, threads=None)