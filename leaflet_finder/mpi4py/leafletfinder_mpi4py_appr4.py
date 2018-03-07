from __future__ import print_function

import sys
import argparse
import numpy as np
import networkx as nx

from mpi4py import MPI
from time import sleep
from time import time
from scipy.spatial.distance import cdist

def find_parcc(windows, index,cutoff=15.0):
    print 'Starting Running'

    num = windows.shape[0]
    
    i_index = index[0]
    j_index = index[1]
    
    graph = nx.Graph()
    if i_index == j_index:
       train = window[0]
       test = window[1]
    else:
        train = np.vstack([windows[0],windows[1]])
        test  = np.vstack([windows[0],windows[1]])
    
    tree = BallTree(train, leaf_size=40)
    edges = tree.query_radius(test, cutoff)
    print 'Query Done'
    edge_list=[list(zip(np.repeat(idx, len(dest_list)), \
            dest_list)) for idx, dest_list in enumerate(edges)]

    edge_list_flat = np.array([list(item) \
            for sublist in edge_list for item in sublist])
    if i_index == j_index:
        res = edge_list_flat.transpose()
        res[0] = res[0] + i_index - 1
        res[1] = res[1] + j_index - 1
    else:
        removed_elements = list()
        for i in range(edge_list_flat.shape[0]):
            if (edge_list_flat[i,0]>=0 and edge_list_flat[i,0]<=num-1) and (edge_list_flat[i,1]>=0 and edge_list_flat[i,1]<=num-1) or\
               (edge_list_flat[i,0]>=num and edge_list_flat[i,0]<=2*num-1) and (edge_list_flat[i,1]>=num and edge_list_flat[i,1]<=2*num-1):
                removed_elements.append(i)
        res = np.delete(edge_list_flat,removed_elements,axis=0).transpose()
        res[0] = res[0] + i_index - 1
        res[1] = res[1] -num + j_index - 1

    edges=[(res[0,k],res[1,k]) for k in range(0,res.shape[1])]
    graph.add_edges_from(edges)

    # partial connected components

    subgraphs = nx.connected_components(graph)
    comp = [g for g in subgraphs]
    graph_time=time()
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
