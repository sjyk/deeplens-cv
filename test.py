from dlstorage.utils.benchmark import *



f = FileSystemStorageManager(TestTagger(), 'videos')
p = PerformanceTest(f, '/Users/sanjaykrishnan/Downloads/BigBuckBunny.mp4')
p.runAll()

vd = VDMSStorageManager(TestTagger())
vd.put('enter actual directory here', 'desired name')
print(vd.get('desired name', TRUE, 438))

