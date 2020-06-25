import sys
import json

import matplotlib.pyplot as plt
import numpy as np

datafile = open(sys.argv[1],'r')
lines = [line.split(';') for line in datafile.readlines()]

plots = {}
for line in lines:
	key = line[0]

	if key not in plots:
		plots[key] = [[],[]]

	settings = json.loads(line[1])
	plots[key][0].append(float(settings['background_scale']))
	plots[key][1].append(float(line[3]))

markers = 'oDsv'
for i,k in enumerate(plots):
	x_pos = np.array([i for i, _ in enumerate(plots[k][0])])
	plt.bar(x_pos+(i-0.5)*0.4, plots[k][1], width=0.4)
	plt.xticks(x_pos, plots[k][0])

plt.style.use('seaborn-paper')
# plt.grid()
plt.xlabel('Background Scale')
plt.ylabel('Put time (s)')
plt.title(sys.argv[2])
plt.legend(plots.keys())
plt.savefig(sys.argv[1]+'_plot.png')
