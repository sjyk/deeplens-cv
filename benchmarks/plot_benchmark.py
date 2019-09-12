import sys
import matplotlib.pyplot as plt
import json

def loadFile(filename, index):
	rtn = []
	buffer = []

	f = open(filename, 'r')
	for line in f.readlines():
		if 'dlstorage' in line:
			rtn.append(buffer)
			buffer = (line,[])
		else:
			buffer[1].append(json.loads(line))

	rtn.append(buffer)

	return rtn[1:][index]

def project(data, xaxis, yaxis):

	title = data[0]
	data = data[1]
	xvals = (xaxis, [ t[xaxis]  for t in data])
	yvals = (yaxis, [ t[yaxis]  for t in data])

	return title, xvals, yvals

def plot(title, xvals, yvals):

	plt.ylabel(yvals[0])
	plt.xlabel(xvals[0])

	plt.title(title)
	plt.grid()
	plt.title(title)
	plt.plot(xvals[1], yvals[1], 's-')
	plt.show()



if __name__== "__main__":
	filename = sys.argv[1]
	index = int(sys.argv[2])
	xaxis = sys.argv[3]
	yaxis = sys.argv[4]

	data = loadFile(filename, index)
	slice = project(data, xaxis, yaxis)
	plot(*slice)



