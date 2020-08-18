import os
import math

def generate_videos(video_width, video_height, shapes, max_pixels, min_pixels, reducing_factor):
    dictionary_of_video_sizes = {}
    reducing_factor = 1/reducing_factor
    for shape in shapes:
        height_temp = video_height
        width_temp = video_width
        key = f"{shape[0]}:{shape[1]}"
        dictionary_of_video_sizes[key] = []
        height_temp = math.ceil((shape[1] / shape[0]) * video_width)
        if height_temp > video_height:
            height_temp = video_height
            width_temp = math.ceil((shape[0] / shape[1]) * video_height)
        dictionary_of_video_sizes[key].append((width_temp, height_temp))
        while width_temp * height_temp > min_pixels:
            height_temp = math.ceil(height_temp * math.sqrt(reducing_factor))
            width_temp = math.ceil(width_temp * math.sqrt(reducing_factor))
            dictionary_of_video_sizes[key].append((width_temp, height_temp))
    return dictionary_of_video_sizes

# Test
#Dict = generate_videos(1920, 1080, [(16,9), (1,1), (1,20), (20,1), (2,3)], 1000000000000, 30000, 2)
#print(Dict)
