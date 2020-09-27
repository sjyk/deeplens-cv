import os
import pandas as pd 
from matplotlib import pyplot as plt 
from aggregate_csv import human_format

def graph_diagnostics(path_to_csv, path_to_agg_csv):
    os.mkdir('results/graphs')
    os.mkdir('results/graphs/spread')
    os.mkdir('results/graphs/file_size')
    os.mkdir('results/graphs/total_time')

    dfo = pd.read_csv(path_to_csv)
    for shape, dfi in dfo.groupby('Shape'):
        storage_time = []
        retrieval_time = []
        sizes = []

        for size, df in dfi.groupby('Size'):
            sizes.append(human_format(size))
            storage_time.append(df['Storage Time'].tolist())
            retrieval_time.append(df['Retrieval Time'].tolist())

        fig, ax = plt.subplots()
        ax.set_title(f"{shape} Storage Time")
        ax.boxplot(storage_time, labels=sizes)
        plt.xlabel("Size (pixels)")
        plt.ylabel("Time (seconds)")
        plt.savefig(f"results/graphs/spread/{shape}_storagetime.png")
        fig.clf()

        fig, ax = plt.subplots()
        ax.set_title(f"{shape} Retrieval Time")
        ax.boxplot(retrieval_time, labels=sizes)
        plt.xlabel("Size (pixels)")
        plt.ylabel("Time (seconds)")
        plt.savefig(f"results/graphs/spread/{shape}_retrievaltime.png")
        fig.clf()

    dfa = pd.read_csv(path_to_agg_csv)
    dfa['Shape'] = dfa['Shape'].apply(lambda x: sum(map(ord, x)) + ord(x[0]))

    fig = plt.figure(figsize = (10, 5)) 
    norm = plt.Normalize(dfa['Shape'].min(), dfa['Shape'].max())
    cmap = plt.get_cmap("magma")
    plt.bar(dfa['Size'], height=dfa['File Size Median'], color=cmap(norm(dfa['Shape'].values))) 
    plt.xlabel("Video Size (pixels)") 
    plt.ylabel("Cache size (bytes)") 
    plt.title(f"File Size") 
    plt.savefig(f"results/graphs/file_size/filesize.png")
    fig.clf()

    fig = plt.figure(figsize = (10, 5))
    norm = plt.Normalize(dfa['Shape'].min(), dfa['Shape'].max())
    cmap = plt.get_cmap("magma")
    p1 = plt.bar(dfa['Size'], height=dfa['Storage Time Median'], color=cmap(norm(dfa['Shape'].values)))
    p2 = plt.bar(dfa['Size'], height=dfa['Retrieval Time Median'], bottom=dfa['Storage Time Median'])
    plt.xlabel("Video Size (pixels)") 
    plt.ylabel("Total Time (seconds)") 
    plt.title(f"Total Time") 
    plt.savefig(f"results/graphs/total_time/totaltime.png")
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

    path_to_graphs = 'results/graphs' # A folder of graphs
    return path_to_graphs

# Test
#graph_diagnostics("results/diagnostics.csv", "results/diagnostics_agg.csv")