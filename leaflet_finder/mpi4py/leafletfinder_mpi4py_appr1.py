import sys
import argparse
import numpy as np
from mpi4py import MPI
from time import sleep
from time import time
from scipy.spatial.distance import cdist


def find_edges(data, cutoff=15.0):
    map_start = time()
    window, index = data[0]
    par_list = (cdist(window[0], window[1]) < cutoff)
    adj_list = np.where(par_list == True)
    adj_list = np.vstack(adj_list)
    adj_list[0] = adj_list[0] + index[0] - 1
    adj_list[1] = adj_list[1] + index[1] - 1
    if adj_list.shape[1] == 0:
        adj_list = np.zeros((2, 1))
    map_return = time()

    return [adj_list, index, map_start, map_return]


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="File with the atoms positions")
    parser.add_argument(
        "cutoff", help="The cutoff value for the edge existence")
    args = parser.parse_args()
    cutoff = int(args.cutoff)

    comm = MPI.COMM_WORLD
    world_size = comm.Get_size()
    proc_rank = comm.Get_rank()

    if proc_rank == 0:
        atoms = np.load(args.file)
    else:
        atoms = None

    atoms = comm.bcast(atoms, root=0)

    indx_start = np.ceil(np.divide(np.double(atoms.shape[0]),world_size)) * proc_rank
    indx_end = np.ceil(np.divide(np.double(atoms.shape[0]),world_size)) * (proc_rank + 1)
    
    print "Rank %d of %d procs has %d up to %d atoms" % (proc_rank, world_size, indx_start, indx_end)
