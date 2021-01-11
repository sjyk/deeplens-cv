import pandas as pd

def aggregate_csv(path_to_csv):
    df = pd.read_csv(path_to_csv)
    df = df.groupby(['Shape','Size']).agg({'File Size': ['min', 'median', 'max'], 'Storage Time': ['min', 'median', 'max'], 'Retrieval Time': ['min', 'median', 'max'], \
                         "Storage CPU Median": ['median'], "Storage CPU Max": ['max'], "Storage RAM Median": ['median'], "Storage RAM Max": ['max'], \
                         "Storage Read Count Median": ['median'], "Storage Read Count Max": ['max'], "Storage Write Count Median": ['median'], "Storage Write Count Max": ['max'], \
                         "Storage Read Bytes Median": ['median'], "Storage Read Bytes Max": ['max'], "Storage Write Bytes Median": ['median'], "Storage Write Bytes Max": ['max'], \
                         "Retrieval CPU Median": ['median'], "Retrieval CPU Max": ['max'], "Retrieval RAM Median": ['median'], "Retrieval RAM Max": ['max'], \
                         "Retrieval Read Count Median": ['median'], "Retrieval Read Count Max": ['max'], "Retrieval Write Count Median": ['median'], "Retrieval Write Count Max": ['max'], \
                         "Retrieval Read Bytes Median": ['median'], "Retrieval Read Bytes Max": ['max'], "Retrieval Write Bytes Median": ['median'], "Retrieval Write Bytes Max": ['max']})
    df.reset_index(inplace=True)
    df.columns = df.columns.get_level_values(0)
    df.columns = ['Shape', 'Size', 'File Size Min', 'File Size Median', 'File Size Max', 'Storage Time Min', 'Storage Time Median', 'Storage Time Max', 'Retrieval Time Min', 'Retrieval Time Median', 'Retrieval Time Max', \
                         "Storage CPU Median", "Storage CPU Max", "Storage RAM Median", "Storage RAM Max", \
                         "Storage Read Count Median", "Storage Read Count Max", "Storage Write Count Median", "Storage Write Count Max", \
                         "Storage Read Bytes Median", "Storage Read Bytes Max", "Storage Write Bytes Median", "Storage Write Bytes Max", \
                         "Retrieval CPU Median", "Retrieval CPU Max", "Retrieval RAM Median", "Retrieval RAM Max", \
                         "Retrieval Read Count Median", "Retrieval Read Count Max", "Retrieval Write Count Median", "Retrieval Write Count Max", \
                         "Retrieval Read Bytes Median", "Retrieval Read Bytes Max", "Retrieval Write Bytes Median", "Retrieval Write Bytes Max"]

    df = df.sort_values(by='Size')
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