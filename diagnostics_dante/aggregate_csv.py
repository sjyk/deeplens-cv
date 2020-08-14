import pandas as pd

def aggregate_csv(path_to_csv):
    df = pd.read_csv(path_to_csv)
    df = df.groupby(['Shape','Size']).agg({'File Size': ['min', 'median', 'max'], 'Storage Time': ['min', 'median', 'max'], 'Retrieval Time': ['min', 'median', 'max']})

    path_to_agg_csv = path_to_csv[:-4] + "_agg.csv"
    df.to_csv(path_to_agg_csv)
    return path_to_agg_csv

# Test
#aggregate_csv("videos/diagnostics.csv")