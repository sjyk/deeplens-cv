
from deeplens.struct import *
from deeplens.full_manager.full_video_processing import *
def print_crops(crops, labels = None):
    for crop in crops:
        print('New Crop~~~~')
        print(crop['bb'].serialize())
        print(crop['label'])
        print(crop['all'])
    if labels:
        print('Labels~~~')
        print(labels)

def main():
    #Testing map
    # Test if cropping of one object works that slightly moves
    # Test if cropping of two objects that slightly move
    # But overlapps each other works
    data = []
    data.append([{'bb': Box(0, 0, 49, 49),  'label': 'test'}])
    data.append([{'bb': Box(1, 1, 50, 50),  'label': 'test'}])
    data.append([{'bb': Box(0, 0, 49, 49),  'label': 'test1'}])
    data.append([{'bb': Box(1, 1, 49, 49),  'label': 'test1'}])
    splitter = CropSplitter()
    map1 = splitter.map(data)
    map2 = splitter.map(data)
    # Test if empty labels join in tagger
    output = splitter.join(map1, map2)
    print_crops(output[0])
    # Test if same labels join in tagger
    # Test if join works with video_io

if __name__ == '__main__':
    main()