import pandas as pd

def aggregate_csv(path_to_csv):
    df = pd.read_csv(path_to_csv)
    df = df.groupby(['Shape','Size']).agg({'File Size': ['min', 'median', 'max'], 'Storage Time': ['min', 'median', 'max'], 'Retrieval Time': ['min', 'median', 'max']})
    df.reset_index(inplace=True)
    df.columns = df.columns.get_level_values(0)
    df.columns = ['Shape', 'Size', 'File Size Min', 'File Size Median', 'File Size Max', 'Storage Time Min', 'Storage Time Median', 'Storage Time Max', 'Retrieval Time Min', 'Retrieval Time Median', 'Retrieval Time Max']

    df['Size'] = df['Size'].apply(lambda x: human_format(x))

    path_to_agg_csv = path_to_csv[:-4] + "_agg.csv"
    df.to_csv(path_to_agg_csv, index=False)
    return path_to_agg_csv

def human_format(num):
    num = float('{:.3g}'.format(num))
    magnitude = 0
    while abs(num) >= 1000:
        magnitude += 1
        num /= 1000.0
    return '{}{}'.format('{:f}'.format(num).rstrip('0').rstrip('.'), ['', 'K', 'M', 'B', 'T'][magnitude])

# Test
#aggregate_csv("results/diagnostics.csv")