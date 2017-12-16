#!/usr/bin/env python
import sys
import numpy as np
from MDAnalysis.core.qcprot import CalcRMSDRotationalMatrix
import time
import pandas as pd

def rmsd(mobile, xref0):
    # """Calculate optimal RMSD for AtomGroup *mobile* onto the coordinates *xref0* centered at the orgin.
    #The coordinates are not changed. No mass weighting.
    # 738 us
    xmobile0 = mobile.positions - mobile.center_of_mass()
    return CalcRMSDRotationalMatrix(xref0.T.astype(np.float64), xmobile0.T.astype(np.float64),mobile.n_atoms, None, None)

def block_rmsd(index,topology, trajectory, xref0, start=None, stop=None, step=None):
    task_start = time.time()
    clone = mda.Universe(topology, trajectory)
    g = clone.atoms[index]

    print("block_rmsd", start, stop, step)

    bsize = stop-start
    results = np.zeros([bsize,2])
    t_comp = np.zeros(bsize)
    t_IO = np.zeros(bsize)

    start1 = time.time()
    start0 = start1
    for iframe, ts in enumerate(clone.trajectory[start:stop:step]):
        start2 = time.time()
        results[iframe, :] = ts.time, rmsd(g, xref0)
        t_comp[iframe] = time.time()-start2
        t_IO[iframe] = start2-start1
        start1 = time.time()

    t_all_frame = time.time()-start0
    t_comp_final = np.mean(t_comp)
    t_IO_final = np.mean(t_comp)
    task_end = time.time()

    return results, t_comp_final, t_IO_final, t_all_frame, task_start, task_end

if __name__ == '__main__':
    longXTC1 = sys.argv[3]
    PSF = sys.argv[4]
    # Check the files in the directory
    u = mda.Universe(PSF, longXTC1)
    print("frames in trajectory ", u.trajectory.n_frames)
    print (len(u.trajectory))
    mobile = u.select_atoms("(resid 1:29 or resid 60:121 or resid 160:214) and name CA")
    index = mobile.indices
    ref0 = u.universe.select_atoms("protein")
    xref0 = ref0.positions-ref0.center_of_mass()
    out = block_rmsd(index,PSF,DCD,xref0,start=int(sys.argv[1]),stop=int(sys.argv[2]),step=1)



