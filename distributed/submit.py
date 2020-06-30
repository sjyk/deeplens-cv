import environ
import argparse

from deeplens.full_manager.full_video_processing import CropSplitter
from deeplens.tracking.background import FixedCameraBGFGSegmenter
from deeplens.full_manager.condition import Condition, deserialize
from deeplens.full_manager.full_manager import *

FOLDER = 'videos'
STORAGE_ARGS = {'encoding': XVID, 'size': -1, 'sample': 1.0, 'offset': 0, 'limit': -1, 'batch_size': 200,
                      'num_processes': 4, 'background_scale': 1}


def put(video, name, segmentation_args):
	manager = FullStorageManager(\
				CustomTagger(\
					FixedCameraBGFGSegmenter(**segmentation_args).segment, \
					batch_size=STORAGE_ARGS['batch_size']), \
				    CropSplitter(), FOLDER)

	manager.put(video, name, args=STORAGE_ARGS, hwang=False)


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description='Process some integers.')
	
	parser.add_argument('video', help='URL of the video to put')
	parser.add_argument('name', help='Local name of the video')

	parser.add_argument('--mt', dest='movement_threshold', type=int, default=25)
	parser.add_argument('--b', dest='blur', type=int, default=21)
	parser.add_argument('--p', dest='movement_prob', type=float, default=0.05)

	data = parser.parse_args()

	segmentation_args = {'movement_threshold': data.movement_threshold,
					     'blur': data.blur,
					     'movement_prob': data.movement_prob}

	put(data.video, data.name, segmentation_args)

	

