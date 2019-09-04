from dlstorage.utils.benchmark import *



f = FileSystemStorageManager(TestTagger(), 'videos')
p = PerformanceTest(f, '/Users/sanjaykrishnan/Downloads/BigBuckBunny.mp4')
p.runAll()


