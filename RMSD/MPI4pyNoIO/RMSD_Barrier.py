#!/usr/bin/env python
from __future__ import print_function, division
import sys
import numpy as np
import MDAnalysis as mda
from MDAnalysis.analysis.align import rotation_matrix
from MDAnalysis.core.qcprot import CalcRMSDRotationalMatrix
import time
from shutil import copyfile
import glob, os
from MDAnalysis import Writer
import mpi4py
from mpi4py import MPI

#---------------------------------------
#MPI.Init

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
#------------------------------------------
j = sys.argv[1]

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
#-----------------------------------------------------------------------
#DCD1 = os.path.abspath(os.path.normpath(os.path.join(os.getcwd(),'1ake_007-nowater-core-dt240ps.dcd')))
#PSF = os.path.abspath(os.path.normpath(os.path.join(os.getcwd(),'adk4AKE.psf')))
# Check the files in the directory
#longXTC = os.path.abspath(os.path.normpath(os.path.join(os.getcwd(),'newtraj.xtc')))
#longXTC1 = os.path.abspath(os.path.normpath(os.path.join(os.getcwd(),'newtraj{}.xtc'.format(j))))

#if rank == 0:
#   copyfile(longXTC, longXTC1)
#   u = mda.Universe(PSF, longXTC1)
#   print(len(u.trajectory))

MPI.COMM_WORLD.Barrier()

start1 = time.time()
#----------------------------------------------------------------------
#u = mda.Universe(PSF, longXTC1)
#mobile = u.select_atoms("(resid 1:29 or resid 60:121 or resid 160:214) and name CA")
#index = mobile.indices
#topology, trajectory = mobile.universe.filename, mobile.universe.trajectory.filename
#ref0 = mobile.universe.select_atoms("protein")
#xref0 = ref0.positions-ref0.center_of_mass()

xref0 = np.random.rand(3341,3)*15
n_frames = 2512200
bsize = int(np.ceil(n_frames / float(size)))

# Create each segment for each process
#for j in range(1,6): # changing files (5 files per block size)
start2 = time.time()
   
frames_seg = np.zeros([size,2], dtype=int)
for iblock in range(size):
    frames_seg[iblock, :] = iblock*bsize, (iblock+1)*bsize

d = dict([key, frames_seg[key]] for key in range(size))  

start, stop = d[rank][0], d[rank][1] 
print("Hello, World! I am process {} of {} with {} and {}.\n".format(rank, size, start, stop))

# Block-RMSD in Parallel
start3 = time.time()
out = block_rmsd(xref0, start=start, stop=stop, step=1) 
#print("Hello, World! I am process {} of {} with {} and {}.\n".format(rank, size, out[1], out[2]))

start4 = time.time()
# Reduction Process

if rank == 0:
   data1 = np.zeros([size*bsize,2], dtype=float)
   data = np.zeros([size,2], dtype=float)
else:
   data1 = None
   data = None

MPI.COMM_WORLD.Barrier()
bar_time = time.time()

comm.Gather(out[0], data1, root=0)
comm.Gather(np.array(out[1:], dtype=float), data, root=0)


start5 = time.time()

# Cost Calculation
init_time = start2-start1
comm_time1 = start3-start2
wait_time = bar_time-start4
comm_time2 = start5-bar_time
comp_time = start4-start3
tot_time = comp_time+comm_time2+comm_time1

tot_time = comm.gather(tot_time, root=0)
init_time = comm.gather(init_time, root=0)
comm_time1 = comm.gather(comm_time1, root=0)
wait_time = comm.gather(wait_time,root=0)
comm_time2 = comm.gather(comm_time2, root=0)
comp_time = comm.gather(comp_time, root=0)

if rank == 0:
   tot_time = tot_time
   init_time = init_time
   comm_time1 = comm_time1
   wait_time = wait_time
   comm_time2 = comm_time2
   comp_time = comp_time
else:
   tot_time = None
   init_time = None
   comm_time1 = None
   comm_time2 = None
   wait_time = None
   comp_time = None


# Storing the data
if rank == 0:
#   os.remove('files/newtraj{}.xtc'.format(j))   
#   os.remove('files/.newtraj{}.xtc_offsets.npz'.format(j))
   print(tot_time, comm_time1, comm_time2)
   np.save('test_output.npz.npy',data1)
   with open('data1.txt', mode='a') as file1:
        for i in range(size):
            file1.write("{} {} {} {} {}\n".format(size, j, i, data[i][0], data[i][1]))
   with open('data2.txt', mode='a') as file2:
        file2.write("{} {} {}\n".format(size, j, tot_time))
        file2.write("{} {} {}\n".format(size, j, init_time))
        file2.write("{} {} {}\n".format(size, j, comm_time1)) 
        file2.write("{} {} {}\n".format(size, j, wait_time))
        file2.write("{} {} {}\n".format(size, j, comm_time2))
        file2.write("{} {} {}\n".format(size, j, comp_time))  
#MPI.Finalize


