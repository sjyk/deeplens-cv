import environ
from multiprocessing import Pool

from deeplens.full_manager.full_manager import *

from deeplens.dataflow.agg import *
from tqdm import tqdm
import sys

def process(src):
	for frame in tqdm(VideoStream(src)):
		pass

# Usage: python3 test_pass.py 1.mp4 2.mp4 http://example.com/3.mp4
if __name__ == '__main__':
	videos = sys.argv[1:]
	p = Pool(4)
	now = time.time()
	p.map(process, videos)
	print("Time spent:", time.time() - now)
