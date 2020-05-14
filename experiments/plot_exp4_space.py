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
	plots[key][0].append(float(settings['size']))
	plots[key][1].append(float(settings['folder_size']) / 1024 / 1024)

markers = 'oDsv'
for i,k in enumerate(plots):
	plt.plot(plots[k][0], plots[k][1],marker=markers[i])

plt.style.use('seaborn-paper')
plt.grid()
plt.xlabel('# Frames')
plt.ylabel('Disk Usage (MB)')
plt.title(sys.argv[2])
plt.legend(plots.keys())
plt.savefig(sys.argv[1]+'_plot.png')
