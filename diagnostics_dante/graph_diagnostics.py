import os
import pandas as pd 
from matplotlib import pyplot as plt 

def graph_diagnostics(path_to_csv, path_to_agg_csv):
    os.mkdir('results/graphs')
    os.mkdir('results/graphs/spread')
    os.mkdir('results/graphs/file_size')
    os.mkdir('results/graphs/total_time')

    dfo = pd.read_csv(path_to_csv)
    for shape, dfi in dfo.groupby('Shape'):
        storage_time = []
        retrieval_time = []

        for size, df in dfi.groupby('Size'):
            storage_time.append(df['Storage Time'].tolist())
            retrieval_time.append(df['Retrieval Time'].tolist())

        fig = plt.figure(figsize = (10, 5)) 
        ax2 = fig.add_axes([0, 0, 1, 1])
        bp2 = ax2.boxplot(storage_time)
        plt.savefig(f"results/graphs/spread/{shape}_storagetime.png")
        fig.clf()

        fig = plt.figure(figsize = (10, 5)) 
        ax3 = fig.add_axes([0, 0, 1, 1])
        bp3 = ax3.boxplot(retrieval_time)
        plt.savefig(f"results/graphs/spread/{shape}_retrievaltime.png")
        fig.clf()

    dfa = pd.read_csv(path_to_agg_csv)
    for shape, df in dfa.groupby('Shape'):
        df = df.reset_index()
        
        fig = plt.figure(figsize = (10, 5)) 
        plt.bar(df['Size'], height=df['File Size Median']) 
        plt.xlabel("Video Size (pixels)") 
        plt.ylabel("Cache size (bytes)") 
        plt.title(f"{shape} File Size") 
        plt.savefig(f"results/graphs/file_size/{shape}_filesize.png")
        fig.clf()

        fig = plt.figure(figsize = (10, 5))
        p1 = plt.bar(df['Size'], height=df['Storage Time Median'])
        p2 = plt.bar(df['Size'], height=df['Retrieval Time Median'], bottom=df['Storage Time Median'])
        plt.xlabel("Video Size (pixels)") 
        plt.ylabel("Total Time (seconds)") 
        plt.title(f"{shape} Total Time") 
        plt.savefig(f"results/graphs/total_time/{shape}_totaltime.png")
        fig.clf()

    # for size, df in dfo.groupby('Size'):
    #     fig0 = plt.figure(figsize = (10, 5))
    #     p1 = plt.bar(df['Shape'], height=df['Storage Time Median'])
    #     p2 = plt.bar(df['Shape'], height=df['Retrieval Time Median'], bottom=df['Storage Time Median'])
    #     plt.xlabel("Shape") 
    #     plt.ylabel("Total Time (seconds)") 
    #     plt.title(f"{size} Total Time") 
    #     plt.savefig(f"results/graphs/{size}_totaltime.png")

    path_to_graphs = 'results/graphs' # A folder of graphs
    return path_to_graphs

# Test
graph_diagnostics("results/diagnostics.csv", "results/diagnostics_agg.csv")