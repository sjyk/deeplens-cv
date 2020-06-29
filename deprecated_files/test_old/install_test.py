from deeplens.utils.benchmark import *
from deeplens.simple_manager.manager import *

f = SimpleStorageManager(TestTagger(), 'videos')
p = PerformanceTest(f, 'http://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4')
#p = PerformanceTest(f, 'f65sec.mp4')
p.runAll()


