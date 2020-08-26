from generate_videos import generate_videos
from test_videos import test_videos
from aggregate_csv import aggregate_csv
from graph_diagnostics import graph_diagnostics

video_path = 'tcam.mp4'
video_width = 1920
video_height = 1080
number_of_trials = 5

shapes = [(16,9), (4,3), (1,1), (20,1), (1,20)] # An array of aspect ratio tuples (width, height)

# These may be replaced by default (8m to 30k reducing by a factor of two)
max_pixels = 8000000 # The max number of pixels per frame (approx)
min_pixels = 30000 # The min number of pixels per frame (approx)
reducing_factor = 2 # Number of pixels in frame reduced by this factor from max_pixels to min_pixels

# Generate videos
dictionary_of_video_sizes = generate_videos(video_width, video_height, shapes, max_pixels, min_pixels, reducing_factor)

# Test videos creating csv of results
path_to_csv = test_videos(video_path, dictionary_of_video_sizes, number_of_trials)

# Aggregate csv by shape and size (min, med, max)
path_to_agg_csv = aggregate_csv(path_to_csv)

# Graph aggregate csv and put results in new folder
path_to_graphs = graph_diagnostics(path_to_csv, path_to_agg_csv)
