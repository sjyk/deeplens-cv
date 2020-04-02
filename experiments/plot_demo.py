# am lazy so am going to keep this simple

import matplotlib.pyplot as plt

import numpy as np
import matplotlib.pyplot as plt

import math

def subcategorybar(X, vals, width=0.8):
    n = len(vals)
    _X = np.arange(len(X))
    bars = []
    for i in range(n):
        bar = plt.bar(_X - width/2. + i/float(n)*width, vals[i], 
                width=width/float(n), align="edge")   
        bars.append(bar)
    plt.xticks(_X, X)
    return bars

naive = [38.70, 0, 150.53, 0, 7.47, 0.24]
naive_opt = [34.71, 0.12,  6.30,  2.67, 0.35, 0.17]
labels = ('VideoStream', 'Crop', 'KeyPoints', 'GC', 'ActivityMetric', 'Filter')
N = len(labels)

plt.style.use('seaborn-paper')
#p1 = plt.bar(ind, naive_opt, width)
#p2 = plt.bar(ind, naive, width, bottom=naive_opt)

plts = subcategorybar(labels, [naive, naive_opt], width=0.8)

plt.ylabel('Time (s)')
plt.xlabel('Operation')
#plt.xticks(ind, labels)
#plt.yticks(np.arange(0, 81, 10))
plt.yscale('log')
plt.legend((plts[0][0], plts[1][0]), ('Naive', 'Optimized'))

plt.show()


 


ind = np.arange(N)    # the x locations for the groups
width = 0.35       # the width of the bars: can also be len(x) sequence

