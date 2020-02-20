import sys
import json

import matplotlib.pyplot as plt
import matplotlib

datafile = open(sys.argv[1],'r')
lines = [line.split(';') for line in datafile.readlines()]

plots = {}
for line in lines:
	key = line[0]

	if key not in plots:
		plots[key] = [[],[]]

	settings = json.loads(line[1])
	plots[key][0].append(float(settings['sel']))
	plots[key][1].append(float(line[3]))

markers = 'oDsv'
for i,k in enumerate(plots):
	plt.plot(plots[k][0], plots[k][1],marker=markers[i])

plt.style.use('seaborn-paper')
plt.grid()
plt.xlabel('Selectivity')
plt.ylabel('Latency (s)')
plt.title(sys.argv[2])
plt.legend(plots.keys())
plt.savefig(sys.argv[1]+'_plot.png')
