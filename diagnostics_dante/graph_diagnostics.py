import os
import pandas as pd 
from matplotlib import pyplot as plt 

def graph_diagnostics(path_to_agg_csv):
    #os.mkdir('results/graphs')

    df = pd.read_csv(path_to_agg_csv)
    
    fig1 = plt.figure(figsize = (10, 5)) 
    plt.bar(df['Size'], height=df['File Size Max'], bottom=df['File Size Min'])   
    plt.xlabel("Video Size (pixels)") 
    plt.ylabel("Cache size (bytes)") 
    plt.title("(Shape) Storage Size") 
    plt.show()

    fig2 = plt.figure(figsize = (10, 5)) 
  
    plt.bar(df['Size'], height=df['Storage Time Max'], bottom=df['Storage Time Min'])   
    plt.xlabel("Video Size (pixels)") 
    plt.ylabel("Storage Time (seconds)") 
    plt.title("(Shape) Storage Time") 
    plt.show()

    fig3 = plt.figure(figsize = (10, 5)) 
  
    plt.bar(df['Size'], height=df['Retrieval Time Max'], bottom=df['Retrieval Time Min'])   
    plt.xlabel("Video Size (pixels)") 
    plt.ylabel("Retrieval Time (seconds)") 
    plt.title("(Shape) Retrieval Time") 
    plt.show()

    fig3 = plt.figure(figsize = (10, 5)) 
  
    p1 = plt.bar(df['Size'], height=df['Storage Time Median'])
    p2 = plt.bar(df['Size'], height=df['Retrieval Time Median'], bottom=df['Storage Time Median'])
    plt.xlabel("Video Size (pixels)") 
    plt.ylabel("Total Time (seconds)") 
    plt.title("(Shape) Total Time") 
    plt.show()

    path_to_graphs = None # A folder of graphs
    return path_to_graphs

# Test
graph_diagnostics("results/diagnostics_agg.csv")