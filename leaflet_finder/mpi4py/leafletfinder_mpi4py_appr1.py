from __future__ import print_function

import sys
import argparse
import numpy as np
import networkx as nx

from mpi4py import MPI
from time import sleep
from time import time
from scipy.spatial.distance import cdist


def find_edges(atoms, index, cutoff=15.0):
    window = atoms[index[0]:index[1], :]
    par_list = (cdist(window, atoms) < cutoff)
    adj_list = np.where(par_list == True)
    adj_list = np.vstack(adj_list)
    adj_list[0] = adj_list[0] + index[0]
    adj_list[1] = adj_list[1] + index[1]
    if adj_list.shape[1] == 0:
        adj_list = np.zeros((2, 1))

    return adj_list


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

    indx_start = np.ceil(
        np.divide(np.double(atoms.shape[0]), world_size)) * proc_rank
    indx_end = np.ceil(
        np.divide(np.double(atoms.shape[0]), world_size)) * (proc_rank + 1)

    indx_end = indx_end if (indx_end < atoms.shape[0]) else atoms.shape[0]

    proc_adj_list = find_edges(
        atoms, [int(indx_start), int(indx_end)], cutoff=cutoff)

    adj_list = comm.gather(proc_adj_list, root=0)

    if proc_rank == 0:
        print('Calculating Connected Components')
        adj_list = np.hstack(adj_list)
        edges = [(adj_list[0, k], adj_list[1, k])
                 for k in range(0, adj_list.shape[1])]
        graph = nx.Graph(edges)
        subgraphs = nx.connected_components(graph)
        indices = [np.sort(list(g)) for g in subgraphs]
        connComp = time()
        np.save('components.npz.npy', indices)

        print("Connected Components Calculated")
