#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import numpy as np
import sys
import networkx as nx


if __name__ == '__main__':



	args = sys.argv[1:]
	NUMBER_OF_TRAJECTORIES = int(sys.argv[1])
	WINDOW_SIZE = int(sys.argv[2])

	#Adjacency matrix 
	adj_matrix = np.zeros((NUMBER_OF_TRAJECTORIES,NUMBER_OF_TRAJECTORIES))

	for i in range(1,NUMBER_OF_TRAJECTORIES+1,WINDOW_SIZE):
		for j in range(i,NUMBER_OF_TRAJECTORIES,WINDOW_SIZE):
			data = open("distances_%d_%d.data" % (i-1,j-1))
			if i==j: #this is the main diagonal so we fill half the array
				for ii in range(0,WINDOW_SIZE-1):
					line = data.readline().strip().split(',')
					line = map(float,line)
					for jj in range(ii+1,WINDOW_SIZE):
						adj_matrix[i+ii-1,j+jj-1] = line[jj-ii-1]
			else:
				for ii in range(0,WINDOW_SIZE):
					line = data.readline().strip().split(',')
					line = map(float,line)
					for jj in range(0,WINDOW_SIZE):
						adj_matrix[i+ii-1,j+jj-1] = line[jj]


	#print adj_matrix
	#print "\n"
	graph = nx.Graph(adj_matrix)
	subgraphs = nx.connected_components(graph)
	indices = [np.sort(g) for g in subgraphs]
	print indices
