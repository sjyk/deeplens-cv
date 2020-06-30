import environ
import argparse

from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.full_manager.condition import Condition, deserialize
from deeplens.full_manager.full_manager import *
from deeplens.struct import *

FOLDER = 'videos'
STORAGE_ARGS = {'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 200,
                      'num_processes': 4, 'background_scale': 1}


def get(name, condition):
	manager = FullStorageManager(\
				CustomTagger(\
					FixedCameraBGFGSegmenter().segment, \
					batch_size=STORAGE_ARGS['batch_size']), \
				    CropSplitter(), FOLDER)

	clips = manager.get(name, condition)
	srcs = []
	for c in clips:
		if isinstance(c, IteratorVideoStream):
			srcs.extend(c.sources)
		else:
			srcs.append(c.src)

	return srcs



if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Process some integers.')
	
	parser.add_argument('name', help='Local name of the video')

	parser.add_argument('--condition', dest='condition', default="{}")

	data = parser.parse_args()

	condition = deserialize(data.condition)

	rtn = get(data.name, condition)

	print('\n'.join(rtn))

	

