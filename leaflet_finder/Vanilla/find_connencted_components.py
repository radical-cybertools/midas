#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import numpy as np
import sys
import networkx as nx
import argparse
from time import time


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("atomNum", help="Total Number of atoms in the system")
    parser.add_argument("win_size", help="Dimension size of the submatrix")
    args = parser.parse_args()

    NUMBER_OF_ATOMS = int(args.atomNum)
    WINDOW_SIZE = int(args.win_size)
    
    start = time()
    
    #Adjacency matrix 
    adj_matrix = np.empty([NUMBER_OF_ATOMS,NUMBER_OF_ATOMS],dtype='bool')

    #Based on the number of atoms and the submatrices size read all the data
    # and put them to the correct position in the Distance matrix.
    for i in range(1,NUMBER_OF_ATOMS+1,WINDOW_SIZE):
        for j in range(i,NUMBER_OF_ATOMS,WINDOW_SIZE):
            data = np.load("distances_%d_%d.npz.npy" % (i-1,j-1))
            adj_matrix[i-1:i-1+WINDOW_SIZE,j-1:j-1+WINDOW_SIZE]=data
    
    read_time = time()

    # Create a graph that has as adjacency matrix the above and find its connected components.
    graph = nx.Graph(adj_matrix)
    subgraphs = nx.connected_components(graph)
    exec_time = time()
    indices = [np.sort(list(g)) for g in subgraphs]
    np.save('components.npz.npy',indices)
    write_time = time()

    print "%f,%f,%f"%(read_time - start, exec_time - read_time, write_time - exec_time)
