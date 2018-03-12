from __future__ import print_function

import sys
import argparse
import numpy as np
import networkx as nx

from mpi4py import MPI
from time import sleep
from time import time
from sklearn.neighbors import BallTree


def find_parcc(windows, index, cutoff=15.0):
    num = windows[0].shape[0]

    i_index = index[0]
    j_index = index[1]

    if i_index == j_index:
        train = windows[0]
        test = windows[1]
    else:
        train = np.vstack([windows[0], windows[1]])
        test = np.vstack([windows[0], windows[1]])

    tree = BallTree(train, leaf_size=40)
    edges = tree.query_radius(test, cutoff)
    edge_list = [list(zip(np.repeat(idx, len(dest_list)),
                          dest_list)) for idx, dest_list in enumerate(edges)]

    edge_list_flat = np.array([list(item)
                               for sublist in edge_list for item in sublist])
    if i_index == j_index:
        res = edge_list_flat.transpose()
        res[0] = res[0] + i_index
        res[1] = res[1] + j_index
    else:
        removed_elements = list()
        for i in range(edge_list_flat.shape[0]):
            if (edge_list_flat[i, 0] >= 0 and edge_list_flat[i, 0] <= num - 1) \
            and (edge_list_flat[i, 1] >= 0 and edge_list_flat[i, 1] <= num - 1) or\
               (edge_list_flat[i, 0] >= num and edge_list_flat[i, 0] <= 2 * num - 1) \
               and (edge_list_flat[i, 1] >= num and edge_list_flat[i, 1] <= 2 * num - 1) or\
               edge_list_flat[i, 0] > edge_list_flat[i, 1]:
                removed_elements.append(i)
        res = np.delete(edge_list_flat, removed_elements, axis=0).transpose()
        res[0] = res[0] + i_index
        res[1] = res[1] - num + j_index

    if res.shape[1] == 0:
        res = np.zeros((2, 1))

    edges = [(res[0, k], res[1, k]) for k in range(0, res.shape[1])]
    graph = nx.Graph(edges)
    # partial connected components
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

        temp_comp = find_parcc([atoms[indx_i_start:indx_i_start + window, :],
                                atoms[indx_j_start:indx_j_start + window, :]],
                               [int(indx_i_start), int(indx_j_start)])
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
