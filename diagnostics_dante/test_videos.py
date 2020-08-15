import os
from diagnostic import diagnostic

def test_videos(path_to_videos, number_of_trials):
    os.mkdir('results')
    path_to_csv = "results/diagnostics.csv"

    import csv
    with open(path_to_csv, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Shape", "Size", "Trial #", "File Size", "Storage Time", "Retrieval Time"])

    directory = path_to_videos
    
    for video_path in os.listdir(directory):
        filename = os.fsdecode(video_path)
        if filename.endswith(".mp4"):
            name = video_path[:-4]
            shape = name.split('_')[0]
            #size = name.split('_')[1]

            # For testing
            size = 10

            for trial_number in range(number_of_trials):
                info = diagnostic(path_to_videos + video_path)
                file_size = info[0]
                time_storage = info[1]
                time_retreive = info[2]
                with open(path_to_csv, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([shape, size, trial_number, file_size, time_storage, time_retreive])

            continue
        else:
            print("There's a snake in my boot!")
            continue
    return path_to_csv

# Test
test_videos("videos/",5)