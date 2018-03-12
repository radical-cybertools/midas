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
    adj_list[0] = adj_list[0] + index[0]
    adj_list[1] = adj_list[1] + index[1]
    if adj_list.shape[1] == 0:
        adj_list = np.zeros((2, 1))

    edges = [(adj_list[0, k], adj_list[1, k])
             for k in range(0, adj_list.shape[1])]
    graph = nx.Graph(edges)
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
    atoms = int(args.atoms)
    window = int(args.window)

    start_time = MPI.Wtime()
    comm = MPI.COMM_WORLD
    world_size = comm.Get_size()
    proc_rank = comm.Get_rank()

    # Responsible for these number of tasks only:
    side_length = atoms / window
    t_tasks = side_length ** 2
    n_tasks = t_tasks / world_size
    r_tasks = range(proc_rank * n_tasks, (proc_rank + 1) * n_tasks)
    atoms = np.load(args.file)
    setup_time = MPI.Wtime()

    proc_comp = list()
    compute_times = list()
    # Iterate over the 'tasks'
    for task_id in r_tasks:
        comp_start_time = MPI.Wtime()
        indx_i_start = (task_id / side_length) * window
        indx_j_start = (task_id % side_length) * window

        temp_comp = find_edges(windows=[atoms[indx_i_start:indx_i_start + window, :],
                                        atoms[indx_j_start:indx_j_start + window, :]],
                               index=[int(indx_i_start), int(indx_j_start)], cutoff=cutoff)
        proc_comp.append(temp_comp)
        compute_times.append((task_id, comp_start_time, MPI.Wtime()))

    comm_str = MPI.Wtime()
    comp_lists = comm.gather(proc_comp, root=0)
    comm_end = MPI.Wtime()

    if proc_rank == 0:
        comp_list_flat = [
            item for a_list in comp_lists for another_list in a_list for item in another_list]
        print('Calculating Connected Components')
        result = list(comp_list_flat)
        while len(result) != 0:
            item1 = result[0]
            result.pop(0)
            ind = []
            for i, item2 in enumerate(comp_list_flat):
                if item1.intersection(item2):
                    item1 = item1.union(item2)
                    ind.append(i)
            ind.reverse()
            [comp_list_flat.pop(j) for j in ind]
            comp_list_flat.append(item1)

        indices = [np.sort(list(g)) for g in comp_list_flat]
        connComp = MPI.Wtime()
        np.save('components.npz.npy', indices)

        print("Connected Components Calculated")

    prof_file = open('profile_rank_%d.txt' % proc_rank, 'w')
    prof_file.write('%f\n' % (start_time))
    prof_file.write('%f\n' % (setup_time))
    prof_file.write('{}\n'.format(compute_times))
    prof_file.write('%f\n' % (comm_str))
    prof_file.write('%f\n' % (comm_end))
    if proc_rank == 0:
        prof_file.write('%f\n' % (connComp))
    prof_file.close()
