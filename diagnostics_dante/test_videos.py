import os
from diagnostic import diagnostic

def test_videos(video_path, dictionary_of_video_sizes, number_of_trials):
    os.mkdir('results')
    path_to_csv = "results/diagnostics.csv"

    import csv
    with open(path_to_csv, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Shape", "Size", "Trial #", "File Size", "Storage Time", "Retrieval Time", \
                         "Storage CPU Median", "Storage CPU Max", "Storage RAM Median", "Storage RAM Max", \
                         "Storage Read Count Median", "Storage Read Count Max", "Storage Write Count Median", "Storage Write Count Max", \
                         "Storage Read Bytes Median", "Storage Read Bytes Max", "Storage Write Bytes Median", "Storage Write Bytes Max", \
                         "Retrieval CPU Median", "Retrieval CPU Max", "Retrieval RAM Median", "Retrieval RAM Max", \
                         "Retrieval Read Count Median", "Retrieval Read Count Max", "Retrieval Write Count Median", "Retrieval Write Count Max", \
                         "Retrieval Read Bytes Median", "Retrieval Read Bytes Max", "Retrieval Write Bytes Median", "Retrieval Write Bytes Max" \
                        ])
    
    print(dictionary_of_video_sizes)
    for shape, sizes in dictionary_of_video_sizes.items():
        for trial_number in range(number_of_trials):
            for size in sizes:
                info = diagnostic(video_path, size)
                file_size = info[0]
                time_storage = info[1]
                time_retreive = info[2]

                cpu_storage_median = info[3]
                cpu_storage_max = info[4]
                ram_storage_median = info[5]
                ram_storage_max = info[6]
                read_count_storage_median = info[7]
                read_count_storage_max = info[8]
                write_count_storage_median = info[9]
                write_count_storage_max = info[10]
                read_bytes_storage_median = info[11]
                read_bytes_storage_max = info[12]
                write_bytes_storage_median = info[13]
                write_bytes_storage_max = info[14]
                
                cpu_retrieval_median = info[15]
                cpu_retrieval_max = info[16]
                ram_retrieval_median = info[17]
                ram_retrieval_max = info[18]
                read_count_retrieval_median = info[19]
                read_count_retrieval_max = info[20]
                write_count_retrieval_median = info[21]
                write_count_retrieval_max = info[22]
                read_bytes_retrieval_median = info[23]
                read_bytes_retrieval_max = info[24]
                write_bytes_retrieval_median = info[25]
                write_bytes_retrieval_max = info[26]

                with open(path_to_csv, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([shape, size[0]*size[1], trial_number, file_size, time_storage, time_retreive, \
                                     cpu_storage_median, cpu_storage_max, ram_storage_median, ram_storage_max, \
                                     read_count_storage_median, read_count_storage_max, write_count_storage_median, write_count_storage_max, \
                                     read_bytes_storage_median, read_bytes_storage_max, write_bytes_storage_median, write_bytes_storage_max, \
                                     cpu_retrieval_median, cpu_retrieval_max, ram_retrieval_median, ram_retrieval_max, \
                                     read_count_retrieval_median, read_count_retrieval_max, write_count_retrieval_median, write_count_retrieval_max, \
                                     read_bytes_retrieval_median, read_bytes_retrieval_max, write_bytes_retrieval_median, write_bytes_retrieval_max, \
                                    ])

    return path_to_csv
