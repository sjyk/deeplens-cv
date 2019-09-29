import matplotlib.pyplot as plt
import numpy as np
import json
import copy
import sys

"""
fname: the name of the file containing the test
results
tname: the name of the file used for the test
We assume tname does not include the extension
"""

class SingleTest():
    """
    This class contains all information about a single test
    """

    
    def __init__(self, \
                 title="", \
                 vdtime=[], \
                 vdfirst_frame=[], \
                 dltime=[], \
                 dlfirst_frame=[]):
        self.title = title
        self.vdtime = vdtime
        self.vdfirst_frame = vdfirst_frame
        self.dltime = dltime
        self.dlfirst_frame = dlfirst_frame
    
    def _print(self):
        print("Title: " + self.title)
        print("VDMS Times")
        print(self.vdtime)
        print("VDMS First-Frame")
        print(self.vdfirst_frame)
        print("DeepLens Times")
        print(self.dltime)
        print("DeepLens First-Frame")
        print(self.dlfirst_frame)

    def _reset(self):
        self.title = ""
        self.vdtime = []
        self.vdfirst_frame = []
        self.dltime = []
        self.dlfirst_frame = []

def extractTimes(vdname, dlname):
    fh = open(vdname, "r")
    reslst = []
    st = SingleTest()
    start = True
    
    for line in fh.readlines():
        if "dlstorage" in line:
            if start == True:
                st.title = line
                start = False
            else:
                reslst.append(st)
                st = copy.deepcopy(st)
                st._reset()
                st.title = line
                
        else:
           json_data = json.loads(line)
           totTime = json_data['time'] 
           if 'first_frame' in json_data:
               first_frame = json_data['first_frame']
               st.vdfirst_frame.append(float(first_frame))
               st.vdtime.append(float(totTime))
           else:
               st.vdtime.append(float(totTime))
    reslst.append(st)
    fh.close()
    print("reslst size: " + str(len(reslst)))
    for r in reslst:
        print("Title: " + r.title)
    fh2 = open(dlname, "r")
    i = -1
    for line in fh2.readlines():
        if "dlstorage" in line:
            i = i+1
        else:
           json_data = json.loads(line)
           totTime = json_data['time'] 
           if 'first_frame' in json_data:
               first_frame = json_data['first_frame']
               reslst[i].dltime.append(float(totTime))
               reslst[i].dlfirst_frame.append(float(first_frame))
           else:
               reslst[i].dltime.append(float(totTime))
    fh2.close()
    for r in reslst:
        print(r.title)
    return reslst

def plotTests(testlst):
    xs = np.arange(5,56,5)
    xs2 = np.arange(0, 61, 10)
    xs3 = np.arange(1,9)
    j = 1
    for i,t in enumerate(testlst):
        #print(t.title)
        if len(t.vdtime) == 1 and len(t.dltime) == 1:
            if len(t.vdfirst_frame) > 0 and len(t.dlfirst_frame) > 0:
                labels = ['VDMS','DeepLens']
                index = np.arange(len(labels))
                firsts = [t.vdfirst_frame[0],t.dlfirst_frame[0]]
                plt.figure(j)
                plt.bar(index,firsts)
                plt.ylabel('Time (s)')
                plt.xticks(index,labels, ha='center')
                plt.title(t.title + ": first_frame")
                plt.savefig('test' + str(i) + 'first_frame.png')
                j = j+1
            labels = ['VDMS','DeepLens']
            index = np.arange(len(labels))
            times = [t.vdtime[0],t.dltime[0]]
            plt.figure(j)
            plt.bar(index,times)
            plt.ylabel('Time (s)')
            plt.xticks(index, labels, ha='center')
            #print(type(t.title))
            plt.title(t.title)
            plt.savefig('test' + str(i) + '.png')
            j = j+1
        elif len(t.vdtime) < 1 or len(t.dltime) < 1:
            #print(t.title)
            print("plotTests ERROR: No times were collected")
            return
        else:
            if len(t.vdtime) == 11:
                if len(t.vdfirst_frame) > 0 and len(t.dlfirst_frame) > 0:
                    plt.figure(j)
                    plt.plot(xs, t.vdfirst_frame)
                    plt.plot(xs, t.dlfirst_frame)
                    plt.title(t.title + ": first_frame")
                    plt.ylabel('Time (s)')
                    plt.xlabel('Clip Size (in Seconds)')
                    plt.legend(['VDMS','DeepLens'], loc='upper left')
                    plt.savefig('test' + str(i) + 'first_frame.png')
                    j = j+1
                plt.figure(j)
                plt.plot(xs,t.vdtime)
                plt.plot(xs,t.dltime)
                plt.title(t.title)
                plt.ylabel('Time (s)')
                plt.xlabel('Clip Size (in Seconds)')
                plt.legend(['VDMS','DeepLens'], loc='upper left')
                plt.savefig('test' + str(i) + '.png')
                j = j+1
            elif len(t.vdtime) == 7:
                if len(t.vdfirst_frame) > 0 and len(t.dlfirst_frame) > 0:
                    plt.figure(j)
                    plt.plot(xs2, t.vdfirst_frame)
                    plt.plot(xs2, t.dlfirst_frame)
                    plt.title(t.title + ": first_frame")
                    plt.ylabel('Time (s)')
                    plt.xlabel('Filter Selectivity (in Seconds)')
                    plt.legend(['VDMS','DeepLens'], loc='upper left')
                    plt.savefig('test' + str(i) + 'first_frame.png')
                    j = j+1
                plt.figure(j)
                plt.plot(xs2,t.vdtime)
                plt.plot(xs2,t.dltime)
                plt.ylabel('Time (s)')
                plt.xlabel('Filter Selectivity (in Seconds)')
                plt.title(t.title)
                plt.legend(['VDMS','DeepLens'], loc='upper left')
                plt.savefig('test' + str(i) + '.png')
                j = j+1
            elif len(t.vdtime) == 3 and len(t.dltime) == 8:
                if len(t.vdfirst_frame) > 0 and len(t.dlfirst_frame) > 0:
                    #pad the VDMS first_frame times
                    pad = [None]*5
                    vdlst = t.vdfirst_frame + pad
                    plt.figure(j)
                    plt.plot(xs3, vdlst)
                    plt.plot(xs3, t.dlfirst_frame)
                    plt.title(t.title + ": first_frame")
                    plt.ylabel('Time (s)')
                    plt.xlabel('Number of Cores')
                    plt.legend(['VDMS','DeepLens'], loc='upper left')
                    plt.savefig('test' + str(i) + 'first_frame.png')
                    j = j+1
                pad = [None]*5
                vdlst = t.vdtime + pad
                plt.figure(j)
                plt.plot(xs3,vdlst)
                plt.plot(xs3,t.dltime)
                plt.ylabel('Time (s)')
                plt.xlabel('Number of Cores')
                plt.title(t.title)
                plt.legend(['VDMS','DeepLens'], loc='upper left')
                plt.savefig('test' + str(i) + '.png')
                j = j+1
                
            

testlst = extractTimes(sys.argv[1],sys.argv[2])
plotTests(testlst)
            
            