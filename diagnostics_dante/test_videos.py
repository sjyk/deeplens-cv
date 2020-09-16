import os
from diagnostic import diagnostic

def test_videos(video_path, dictionary_of_video_sizes, number_of_trials):
    os.mkdir('results')
    path_to_csv = "results/diagnostics.csv"

    import csv
    with open(path_to_csv, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Shape", "Size", "Trial #", "File Size", "Storage Time", "Retrieval Time", "Storage Usage Median", "Storage Usage Max", "Retrieval Usage Median", "Retrieval Usage Max"])
    
    print(dictionary_of_video_sizes)
    for shape, sizes in dictionary_of_video_sizes.items():
        for trial_number in range(number_of_trials):
            for size in sizes:
                info = diagnostic(video_path, size)
                file_size = info[0]
                time_storage = info[1]
                time_retreive = info[2]
                usage_storage_median = info[3]
                usage_storage_max = info[4]
                usage_retrieve_median = info[5]
                usage_retrieve_max = info[6]
                with open(path_to_csv, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([shape, size[0]*size[1], trial_number, file_size, time_storage, time_retreive, usage_storage_median, usage_storage_max, usage_retrieve_median, usage_retrieve_max])

    return path_to_csv
