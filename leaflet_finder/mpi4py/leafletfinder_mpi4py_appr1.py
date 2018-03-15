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

    # Calculate the adjacency list for the part of the dataset seen.
    window = atoms[index[0]:index[1], :]
    par_list = (cdist(window, atoms) < cutoff)
    adj_list = np.where(par_list == True)
    adj_list = np.vstack(adj_list)

    # Adjust the index of based on where the executing window lies.
    adj_list[0] = adj_list[0] + index[0]

    # if there is no edge, just return an edge from node to 0 to itself.
    if adj_list.shape[1] == 0:
        adj_list = np.zeros((2, 1))

    return adj_list


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="File with the atoms positions")
    parser.add_argument(
        "cutoff", help="The cutoff value for the edge existence")
    parser.add_argument('n_atoms', help='The number of atoms in the dataset')
    parser.add_argument('window', help="i coordinate at the distance matrix")
    args = parser.parse_args()
    cutoff = int(args.cutoff)
    n_atoms = int(args.n_atoms)
    window = int(args.window)

    start_time = MPI.Wtime()

    comm = MPI.COMM_WORLD
    world_size = comm.Get_size()
    proc_rank = comm.Get_rank()

    # Setup the part of the trajectory that each process is responsible to operate
    # upon.
    t_tasks = n_atoms / window
    n_tasks = t_tasks / world_size
    r_tasks = range(proc_rank * n_tasks, (proc_rank + 1) * n_tasks)

    setup_time = MPI.Wtime()

    # Only process 0 loads the file and broadcasts it to the rest of the
    # processes.
    if proc_rank == 0:
        atoms = np.load(args.file)
    else:
        atoms = None

    atoms = comm.bcast(atoms, root=0)
    broadcast_time = MPI.Wtime()

    proc_adj_list = list()
    compute_times = list()

    # Iterate over the 'tasks'. Each iteration is the equivalent of a task from
    # the task-parallel implementations.
    for task_id in r_tasks:
        comp_start_time = MPI.Wtime()

        # The part of the dataset that is the starting point and the ending point
        # of this task.
        indx_start = task_id * window
        indx_end = indx_start + window

        temp_adj_list = find_edges(
            atoms, [int(indx_start), int(indx_end)], cutoff=cutoff)
        proc_adj_list.append(temp_adj_list)
        compute_times.append((task_id, comp_start_time, MPI.Wtime()))

    # Communicate to rank 0 all the results, through a collective gather.
    comm_str = MPI.Wtime()
    adj_list = comm.gather(proc_adj_list, root=0)
    comm_end = MPI.Wtime()

    # Only Rank 0 is responsible to calculate the connected components of the graph.
    # After completion the result is saved to a file
    # TODO: Make the filename configurable.
    if proc_rank == 0:

        # adj_list is a list of lists of sets. It need to be flatten to a list of
        # sets.
        adj_list_flat = [item for a_list in adj_list for item in a_list]
        print('Calculating Connected Components')
        adj_list = np.hstack(adj_list_flat)
        edges = [(adj_list[0, k], adj_list[1, k])
                 for k in range(0, adj_list.shape[1])]
        graph = nx.Graph(edges)
        subgraphs = nx.connected_components(graph)
        indices = [np.sort(list(g)) for g in subgraphs]
        connComp = MPI.Wtime()
        np.save('components.npz.npy', indices)

        print("Connected Components Calculated")

    # All processes write profile files including all compute times for each
    # task.
    prof_file = open('profile_rank_%d.txt' % proc_rank, 'w')
    prof_file.write('%f\n' % (start_time))
    prof_file.write('%f\n' % (setup_time))
    prof_file.write('%f\n' % (broadcast_time))
    prof_file.write('{}\n'.format(compute_times))
    prof_file.write('%f\n' % (comm_str))
    prof_file.write('%f\n' % (comm_end))
    if proc_rank == 0:
        prof_file.write('%f\n' % (connComp))
    prof_file.close()
