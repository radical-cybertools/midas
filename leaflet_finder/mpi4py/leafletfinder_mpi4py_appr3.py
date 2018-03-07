from __future__ import print_function

import sys
import argparse
import numpy as np
import networkx as nx

from mpi4py import MPI
from time import sleep
from time import time
from scipy.spatial.distance import cdist


def find_edges(windows, index, cutoff=15.0):

    par_list = (cdist(windows[0], windows[1]) < cutoff)
    adj_list = np.where(par_list == True)
    adj_list = np.vstack(adj_list)
    adj_list[0] = adj_list[0] + index[0] - 1
    adj_list[1] = adj_list[1] + index[1] - 1
    if adj_list.shape[1] == 0:
        adj_list = np.zeros((2, 1))

    edges=[(adj_list[0,k],adj_list[1,k]) for k in range(0,adj_list.shape[1])]
    graph = nx.Graph()
    graph.add_edges_from(edges)
    subgraphs = nx.connected_components(graph)
    comp = [g for g in subgraphs]

    return comp


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="File with the atoms positions")
    parser.add_argument(
        "cutoff", help="The cutoff value for the edge existence")
    parser.add_argument('atoms', help='The number of atoms in the dataset')
    parser.add_argument('window', help="i coordinate at the distance matrix")
    args = parser.parse_args()
    cutoff = int(args.cutoff)
    atoms = int(agrs.atoms)
    window = int(args.window)

    comm = MPI.COMM_WORLD
    world_size = comm.Get_size()
    proc_rank = comm.Get_rank()

    # Responsible for these number of tasks only:
    side_length = atom / window
    t_tasks = side_length ** 2
    n_tasks = t_tasks / world_size
    r_tasks = range(rank * n_tasks, (rank + 1) * n_tasks)

    proc_comp = list()

    # Iterate over the 'tasks'
    for task_id in r_tasks:
        indx_i_start = (task_id / side_length) * window
        indx_j_start = (task_id % side_length) * window

        temp_comp = find_edges([atoms[indx_i_start:indx_i_start + window, :],
                               atoms[indx_j_start:indx_j_start + window, :]],
                              [int(indx_i_start), int(indx_j_start)])
        proc_comp.append(temp_adj_list)

    comp_lists = comm.gather(proc_comp, root=0)

    if proc_rank == 0:
        print('Calculating Connected Components')
        result = list(comp_lists)
        while len(result)!=0:
            item1 = result[0]
            result.pop(0)
            ind = []
            for i, item2 in enumerate(comp_lists):
                if item1.intersection(item2):
                    item1=item1.union(item2)
                    ind.append(i)
            ind.reverse()
            [comp_lists.pop(j) for j in ind]
            comp_lists.append(item1)

        indices = [np.sort(list(g)) for g in Components]
        np.save('components.npz.npy', indices)

        print("Connected Components Calculated")
