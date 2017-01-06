#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import numpy as np
import sys
import networkx as nx
import matplotlib.pyplot as pplt


if __name__ == '__main__':



    args = sys.argv[1:]
    NUMBER_OF_TRAJECTORIES = int(sys.argv[1])
    WINDOW_SIZE = int(sys.argv[2])
  
    #Adjacency matrix 
    adj_matrix = np.empty([NUMBER_OF_TRAJECTORIES,NUMBER_OF_TRAJECTORIES],dtype='bool')

    for i in range(1,NUMBER_OF_TRAJECTORIES+1,WINDOW_SIZE):
        for j in range(i,NUMBER_OF_TRAJECTORIES,WINDOW_SIZE):
            data = np.load("distances_%d_%d.npz.npy" % (i-1,j-1))
            adj_matrix[i-1:i-1+WINDOW_SIZE,j-1:j-1+WINDOW_SIZE]=data

    graph = nx.Graph(adj_matrix)
    nx.write_adjlist(graph, 'graph.adjlist')
    subgraphs = nx.connected_components(graph)
    indices = [np.sort(list(g)) for g in subgraphs]
    np.save('components.npz.npy',indices)
