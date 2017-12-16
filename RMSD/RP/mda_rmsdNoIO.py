#!/usr/bin/env python
import sys
import numpy as np
from MDAnalysis.core.qcprot import CalcRMSDRotationalMatrix
import time

def rmsd(xref0):
    # """Calculate optimal RMSD for AtomGroup *mobile* onto the coordinates *xref0* centered at the orgin.
    #The coordinates are not changed. No mass weighting.
    # 738 us
#    xmobile0 = mobile.positions - mobile.center_of_mass()
    xmobile0 = np.random.rand(146,3)*15
    return CalcRMSDRotationalMatrix(xref0.T.astype(np.float64), xmobile0.T.astype(np.float64), 146, None, None)

def block_rmsd(xref0, start=None, stop=None, step=None):
#    clone = mda.Universe(topology, trajectory)
#    g = clone.atoms[index]

    print("block_rmsd", start, stop, step)

    bsize = int(stop-start)
    results = np.zeros([bsize,2], dtype=float)
    t_comp = np.zeros(bsize, dtype=float)

    start1 = time.time()
    for iframe, ts in enumerate(range(start,stop,step)):
        start2 = time.time()
        results[iframe, :] = ts, rmsd(xref0)
        t_comp[iframe] = time.time()-start2

    t_all_frame = time.time()-start1
    t_comp_final = np.mean(t_comp)

#    print("Hello, World! I am process {} of {} with {} and {}.\n".format(rank, size, t_comp_final, t_all_frame))

    return results, t_comp_final, t_all_frame

if __name__ == '__main__':
    
    xref0 = np.random.rand(3341,3)*15

    out = block_rmsd(xref0, start=int(sys.argv[1]), stop=int(sys.argv[2]), step=1)
    #print("Hello, World! I am process {} of {} with {} and {}.\n".format(rank, size, out[1], out[2]))
