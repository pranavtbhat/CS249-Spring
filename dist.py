#!/anaconda/bin/python

import time
import sys
import os
import numpy as np
import Queue
import threading


"""
Queue shared by all the threads
"""
q = Queue.Queue()

"""
Write Thread. This thread picks up rows from the
queue and dumps them into files
"""
fprefix = "file-" # File Prefix (CONSTANT)
fs      = 1       # File suffix (VARIABLE)
fext    = ".dat"  # File extension (CONSTANT)

def getNextFileName():
    global fs, fprefix, fext
    fname = dprefix + "/" + fprefix + str(fs) + fext
    fs += 1
    return fname

def file_writer(dprefix="data/", records_per_file=1000):
    global q
    fcount = 0       # Number of records in current file
    fname = getNextFileName()
    fp = open(fname, "w")

    while True:
        row = q.get()     # Blocking call

        # Decide if we need to change file
        if fcount == records_per_file:
            fp.close()
            fname = getNextFileName()
            fp = open(fname, "w")
            fcount = 0

        # Append row to file
        fp.write("%s , %s \n" % row)
        fp.flush()
        fcount += 1


"""
Random number producing thread.
Given a random generation function,
this thread will keep generating numbers,
and pushing them into the queue.
"""
def generate(algo = np.random.random, desc = "Uniform", **kwargs):
    global q
    while True:
        # Sleep
        time.sleep(0.01)
        row = (desc, algo(**kwargs))
        q.put( row )

tconsumer = None
tuniform = None
tgeometric = None
tpower = None
tnormal = None

try:
    # Start consumer process
    dprefix = sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.realpath(__file__))
    print "Destination directory set to ", dprefix

    tconsumer = threading.Thread(target = file_writer, kwargs = {"dprefix" : dprefix})
    tconsumer.start()

    # Start producer processes
    tuniform = threading.Thread(target = generate, kwargs = {"algo" : np.random.uniform, "desc" : "Uniform", "low" : 0.0, "high" : 1.0})
    tuniform.start()

    tgeometric = threading.Thread(target = generate, kwargs = {"algo" : np.random.geometric, "desc" : "Geometric", "p" : 0.35})
    tgeometric.start()

    tpower = threading.Thread(target = generate, kwargs = {"algo" : np.random.power, "desc" : "Power", "a" : 3})
    tpower.start()

    tnormal = threading.Thread(target = generate, kwargs = {"algo" : np.random.normal, "desc" : "Normal", "loc" : 0.5, "scale" : 0.1})
    tnormal.start()

except (KeyboardInterrupt, SystemExit):
    tconsumer.stop()
    tuniform.stop()
    tgeometric.stop()
    tpower.stop()
    sys.exit()
