from dlstorage.filesystem.manager import *
from dlstorage.constants import *
from dlstorage.utils.debug import *

f = FileSystemStorageManager(TestTagger(), 'videos')
f.put('/Users/sanjaykrishnan/Downloads/BigBuckBunny.mp4', 'bunny')
print(f.get('bunny', TRUE, 60*30))


