from __future__ import division
import sys
from pyspark import SparkContext,SparkConf
from datetime import datetime
import time
import numpy as np
import subprocess
import os
import sys
from shutil import copyfile
import glob

def rmsd(xref0):
    # """Calculate optimal RMSD for AtomGroup *mobile* onto the coordinates *xref0* centered at the orgin.
    #The coordinates are not changed. No mass weighting.
    # 738 us

    from MDAnalysis.analysis.align import rotation_matrix
    from MDAnalysis.core.qcprot import CalcRMSDRotationalMatrix
    
    xmobile0 = np.random.rand(146,3)*15
    return CalcRMSDRotationalMatrix(xref0.T.astype(np.float64), xmobile0.T.astype(np.float64),xmobile0.shape[0], None, None)

def block_rmsd(xref0, start=None, stop=None, step=None):
  
    bsize = stop-start
    results = np.zeros([bsize,2])

    for iframe, ts in enumerate(range(start,stop,step)):
        results[iframe,:] = ts,rmsd( xref0)

    return results

def parallelMap(start=None,stop=None,step=None):
    import numpy as np

    xref0 = np.random.rand(3341,3)
    res = block_rmsd(xref0, start=start, stop=stop, step=step)
    return res

if __name__=="__main__":

    cores = int(sys.argv[2])
    n_frames = 104675*cores
    bsize = int(np.ceil(n_frames / float(cores)))
    for i in range(10):
        sc = SparkContext(appName='RMSDrandom')
        start = time.time()
        dist_Matrix = sc.parallelize(range(cores),len(range(cores)))
        out = dist_Matrix.map(lambda x :parallelMap(start=x*bsize,stop=(x+1)*bsize,step=1)).collect()
        dur = time.time()-start
        sc.stop()
        print 'Iter %d: %f\n'%(cores,dur)
