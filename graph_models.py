import numpy as np
import matplotlib.pyplot as plt

def plot4Models(xs, \
                naives, \
                simples, \
                fulls, \
                parallels, \
                xlabel, \
                ylabel, \
                testname, \
                isLine=True):
    if isLine:
        plt.plot(xs, naives, color='red', label='Naive Model')
        plt.plot(xs, simples, color='blue', label='Simple Model')
        plt.plot(xs, fulls, color='green', label='Full Model')
        plt.plot(xs, parallels, color='orange', label='Full with Parallelism')
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.legend(loc='upper left')
        plt.show()
        #if this generates the kind of graph we want, we'll want to save the figure
        #instead of showing it. Here's the command for that:
        #NOTE: you will need to comment out the above plt.show() in that case
        #plt.savefig(testname)
    else:
        plt.scatter(xs, naives, color='red', label='Naive Model')
        plt.scatter(xs, simples, color='blue', label='Simple Model')
        plt.scatter(xs, fulls, color='green', label='Full Model')
        plt.scatter(xs, parallels, color='orange', label='Full with Parallelism')
        plt.legend(loc='upper left')
        plt.show()
        #if this generates the kind of graph we want, we'll want to save the figure
        #instead of showing it. Here's the command for that:
        #NOTE: you will need to comment out the above plt.show() in that case
        #plt.savefig(testname)

lst = range(10)
xs = np.array(lst)
naives = 5*xs
simples = 4*xs
fulls = 3 * xs
parallels = 2*xs
plot4Models(xs, naives, simples, fulls, parallels, 'Size (MB)', 'Latency (ms)', 'test_graph')

