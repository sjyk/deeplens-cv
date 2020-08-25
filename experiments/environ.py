import os,sys,inspect, shutil
import json
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir) 


def time_filter(start, end):

	def do_filter(conn, video_name):
		c = conn.cursor()
		c.execute("SELECT clip_id FROM clip WHERE ((start_time > %s AND start_time < %s) OR (end_time > %s AND end_time < %s) ) AND video_name = '%s'" % (str(start),str(end), str(start),str(end), video_name))
		return [cl[0] for cl in c.fetchall()]

	return do_filter

def overlap(s1,e1, s2, e2):
	r1 = set(range(s1,e1+1))
	r2 = set(range(s2,e2+1))
	return (len(r1.intersection(r2)) > 1)


def cleanUp():
	if os.path.exists('/tmp/videos'):
		shutil.rmtree('/tmp/videos')


def logrecord(baseline,settings,operation,measurement,*args):
	print(';'.join([baseline, json.dumps(settings), operation, measurement] + list(args)), flush=True)

def do_experiments(data, baseline_lst, size, sels):
	for si in sels:
		s = si/10
		for b in baseline_lst:
			b(data,tot=size, sel=s)

def do_experiments_batch_size(data, baseline_lst, size, batch_sizes):
	for i in batch_sizes:
		for b in baseline_lst:
			b(data,tot=size, batch_size=i)


def do_experiments_size(data, baseline_lst, sizes, sel):
	for si in sizes:
		s = sel
		for b in baseline_lst:
			b(data,tot=si, sel=s)
