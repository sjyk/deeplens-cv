import os
import pandas as pd 
from matplotlib import pyplot as plt 

def graph_diagnostics(path_to_agg_csv):
    os.mkdir('results/graphs')

    dfo = pd.read_csv(path_to_agg_csv)

    for size, df in dfo.groupby('Size'):
        fig0 = plt.figure(figsize = (10, 5))
        p1 = plt.bar(df['Shape'], height=df['Storage Time Median'])
        p2 = plt.bar(df['Shape'], height=df['Retrieval Time Median'], bottom=df['Storage Time Median'])
        plt.xlabel("Shape") 
        plt.ylabel("Total Time (seconds)") 
        plt.title(f"{size} Total Time") 
        plt.savefig(f"results/graphs/{size}_totaltime.png")

    for shape, df in dfo.groupby('Shape'):
        fig1 = plt.figure(figsize = (10, 5)) 
        plt.bar(df['Size'], height=df['File Size Max'], bottom=df['File Size Min']) 
        plt.xlabel("Video Size (pixels)") 
        plt.ylabel("Cache size (bytes)") 
        plt.title(f"{shape} File Size") 
        plt.savefig(f"results/graphs/{shape}_filesize.png")

        fig2 = plt.figure(figsize = (10, 5)) 
        plt.bar(df['Size'], height=df['Storage Time Max'], bottom=df['Storage Time Min'])   
        plt.xlabel("Video Size (pixels)") 
        plt.ylabel("Storage Time (seconds)") 
        plt.title(f"{shape} Storage Time") 
        plt.savefig(f"results/graphs/{shape}_storagetime.png")

        fig3 = plt.figure(figsize = (10, 5)) 
        plt.bar(df['Size'], height=df['Retrieval Time Max'], bottom=df['Retrieval Time Min'])   
        plt.xlabel("Video Size (pixels)") 
        plt.ylabel("Retrieval Time (seconds)") 
        plt.title(f"{shape} Retrieval Time") 
        plt.savefig(f"results/graphs/{shape}_retrievaltime.png")

        fig3 = plt.figure(figsize = (10, 5))
        p1 = plt.bar(df['Size'], height=df['Storage Time Median'])
        p2 = plt.bar(df['Size'], height=df['Retrieval Time Median'], bottom=df['Storage Time Median'])
        plt.xlabel("Video Size (pixels)") 
        plt.ylabel("Total Time (seconds)") 
        plt.title(f"{shape} Total Time") 
        plt.savefig(f"results/graphs/{shape}_totaltime.png")

    path_to_graphs = 'results/graphs' # A folder of graphs
    return path_to_graphs

# Test
graph_diagnostics("results/diagnostics_agg.csv")