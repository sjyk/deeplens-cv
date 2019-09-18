import sys
import os
import json

"""
fname: the name of the file containing the test
results
tname: the name of the file used for the test
We assume tname does not include the extension
"""
def extractTimes(fname,tname):
    if "dl" in fname:
       resname = "dl" + tname + "test.csv" 
    else:
       resname = "vdms" + tname + "test.csv"
    resfile = open(resname, "w+")
    fh = open(fname, "r")
    for line in fh.readlines():
        if "dlstorage" in line:
           resfile.write(line)
           resfile.write("time,first_frame\n")
        else:
           json_data = json.loads(line)
           totTime = json_data['time'] 
           if 'first_frame' in json_data:
              first_frame = json_data['first_frame']
              resfile.write(str(totTime) + "," + str(first_frame) + "\n")
           else:
              resfile.write(str(totTime) + ", \n")
    fh.close()
    resfile.close()
              
extractTimes("vdout5.txt","BigBuckBunny")
