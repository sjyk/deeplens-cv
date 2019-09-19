import os,sys,inspect
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from dlstorage.utils.benchmark import *

filename = sys.argv[1]
threads = int(sys.argv[2])
f = FileSystemStorageManager(TestTagger(), 'videos')
b = BulkTest(f, filename, threads)
b.putOneHr()


