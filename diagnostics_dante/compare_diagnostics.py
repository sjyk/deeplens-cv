# Script that compares different results as line graph
# Take all folders in path folder
# Go in folder -> go in results -> take aggregate csv file
# Make a pandas df
# separate by shape
# add label that says what results they are from
# graph?
# add key and x, y, title

import os
import pandas as pd
import matplotlib.pyplot as plt

def graph_compare(path_to_many_results, shapes):
    os.mkdir('comparison_results')
    results = []
    for dirpath, dirnames, filenames in os.walk(path_to_many_results):
        for fname in filenames:
            if fname == "diagnostics_agg.csv":
                agg_path = dirpath + '/' + fname
                name = dirpath.replace(path_to_many_results + '/', '').replace('/results','')
                results.append((name, agg_path))
    
    dict_data_by_shapes = {}
    for shape in shapes:
        key = f"{shape[0]}:{shape[1]}"
        dict_data_by_shapes[key] = []

    for result in results:
        name = result[0]
        dfo = pd.read_csv(result[1])
        dfo['Total Time'] = dfo.apply(lambda x: x['Retrieval Time Median'] + x['Storage Time Median'], axis=1)
        for shape, df in dfo.groupby('Shape'):
            sizes = df['Size'].tolist()
            data = df['Total Time'].tolist()
            dict_data_by_shapes[shape].append((name, sizes, data))
    
    for shape in dict_data_by_shapes:
        fig = plt.figure(figsize = (10, 5))
        results = dict_data_by_shapes[shape]
        for result in results:
            plt.plot(result[1], result[2], label=result[0])
        plt.xlabel('Frame Size (pixels)')
        plt.ylabel('Total Time (seconds)')
        plt.title(f'Total Time of {shape} results')
        plt.legend()
        plt.savefig(f"comparison_results/{shape}_totaltime.png")





# Test
#graph_compare("../../Results", [(16,9), (4,3), (1,1), (10,1), (1,10), (20,1), (1,20)])