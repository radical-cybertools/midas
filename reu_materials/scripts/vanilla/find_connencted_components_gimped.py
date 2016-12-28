#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import numpy as np
import sys
import math
import networkx as nx
import matplotlib.pyplot as pplt


if __name__ == '__main__':



    args = sys.argv[1:]
    NUMBER_OF_TRAJECTORIES = int(sys.argv[1])
    window_size = int(sys.argv[2])
  
    #Adjacency matrix 
    adj_matrix = np.empty([NUMBER_OF_TRAJECTORIES,NUMBER_OF_TRAJECTORIES],dtype='bool')

    smallest_line_count = NUMBER_OF_TRAJECTORIES%window_size

    for elem in range(0,378):
        data = np.load("distances_%d.npz.npy" % (elem))
        if elem == 377:
            adj_matrix[elem*window_size:elem*window_size+smallest_line_count,:]= data
        else:
            adj_matrix[elem*window_size:(elem+1)*window_size,:] = data

    graph = nx.Graph(adj_matrix)
    nx.write_adjlist(graph, 'graph.adjlist')
    subgraphs = nx.connected_components(graph)
    indices = [np.sort(list(g)) for g in subgraphs]
    np.save('components.npz.npy',indices)
