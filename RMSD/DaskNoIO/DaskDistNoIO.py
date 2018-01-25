#!/usr/bin/env python
from __future__ import print_function, division
import sys
import numpy as np
from dask.delayed import delayed
from MDAnalysis.core.qcprot import CalcRMSDRotationalMatrix
import time
from shutil import copyfile
import glob, os
from dask.distributed import Client
from distributed.diagnostics.plugin import SchedulerPlugin

def submitCustomProfiler(profname,dask_scheduler):
    prof = MyProfiler(profname)
    dask_scheduler.add_plugin(prof)

def removeCustomProfiler(dask_scheduler):
    dask_scheduler.remove_plugin(dask_scheduler.plugins[-1])

class MyProfiler(SchedulerPlugin):
    def __init__(self,profname):
        self.profile = profname

    def transition(self,key,start,finish,*args,**kargs):
        if start == 'processing' and finish == 'memory':
            with open(self.profile,'a') as ProFile:
                ProFile.write('{}\n'.format([key,start,finish,kargs['worker'],kargs['thread'],kargs['startstops']]))


def rmsd(xref0):
    # """Calculate optimal RMSD for AtomGroup *mobile* onto the coordinates *xref0* centered at the orgin.
    #The coordinates are not changed. No mass weighting.
    # 738 us
    xmobile0 = np.random.rand(146,3)*15
    return CalcRMSDRotationalMatrix(xref0.T.astype(np.float64), xmobile0.T.astype(np.float64),xmobile0.shape[0], None, None)

def block_rmsd(xref0, start=None, stop=None, step=None):
    
    print("block_rmsd", start, stop, step)

    bsize = stop-start
    results = np.zeros([bsize,2])
    t_comp = np.zeros(bsize)

    start1 = time.time()
    for iframe, ts in enumerate(range(start,stop,step)):
        start2 = time.time()
        results[iframe,:] = ts,rmsd( xref0)
        t_comp[iframe] = time.time()-start2

    t_all_frame = time.time()-start1
    t_comp_final = np.mean(t_comp)

    return results, t_comp_final, t_all_frame

def com_parallel_dask_distributed(n_frames, n_blocks):
    xref0 = np.random.rand(3341,3)
    bsize = int(np.ceil(n_frames / float(n_blocks)))
    print("setting up {} blocks with {} frames each".format(n_blocks, bsize))

    blocks = []
    t_comp = []
    t_all_frame = []

    for iblock in range(n_blocks):
        start, stop, step = iblock*bsize, (iblock+1)*bsize, 1
        print("dask setting up block trajectory[{}:{}]".format(start, stop))

        out = delayed(block_rmsd, pure=True)(xref0, start=start, stop=stop, step=step)

        blocks.append (out[0])
        t_comp.append (out[1])
        t_all_frame.append (out[2])

    total = delayed(np.vstack)(blocks)
    t_comp_avg = delayed(np.sum)(t_comp)/n_blocks
    t_comp_max = delayed(np.max)(t_comp)
    t_all_frame_avg = delayed(np.sum)(t_all_frame)/n_blocks
    t_all_frame_max = delayed(np.max)(t_all_frame)

    return total, t_comp_avg, t_comp_max, t_all_frame_avg, t_all_frame_max


if __name__ == '__main__':
    Scheduler_IP = sys.argv[1]
    #SLURM_JOBID = sys.argv[2]
    print (Scheduler_IP)
    #print (Client(Scheduler_IP))
    c = Client(Scheduler_IP)

    with open('data3.txt', mode='w') as file:
        traj_size = [600]
        for k in traj_size: # we have 3 trajectory sizes
            block_size = [int(sys.argv[2])]
            for i in block_size:      # changing blocks
                for j in range(10):    # changing files (5 files per block size)
                    c.run_on_scheduler(submitCustomProfiler,sys.argv[3]+'/stragglers_test_%d_%d_%d.txt'%(k,i,j))
                    # Provide the path to my file to all processes
                    total = com_parallel_dask_distributed(104675*i, i)
                    total = delayed (total)
                    start = time.time()
                    output = total.compute(get=c.get)
                    tot_time = time.time()-start
                    c.run_on_scheduler(removeCustomProfiler)
                    file.write('size,blocks,iter,t_comp_avg,t_comp_max,t_all_frame_avg,t_all_frame_max,tot_time')
                    file.write("{0},{1},{2},{3},{4},{5},{6},{7}\n".format(k, i, j, output [1], output [2], output [3], output [4], tot_time))
                    file.flush()
