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
            # change to be for every col
            data = df["Retrieval Write Count Median"].tolist()
            dict_data_by_shapes[shape].append((name, sizes, data))

    for shape in dict_data_by_shapes:
        fig = plt.figure(figsize = (10, 5))
        results = dict_data_by_shapes[shape]
        for result in results:
            plt.plot(result[1], result[2], label=result[0])
        plt.xlabel('Frame Size (pixels)')
        plt.ylabel("Storage Read Count Median")
        plt.title(f'"Storage Read Count Median" of {shape} results')
        plt.legend()
        plt.savefig(f"comparison_results/{shape}_retrievalwritecount.png")





# Test
#graph_compare("../../Results Usage", [(16,9)])