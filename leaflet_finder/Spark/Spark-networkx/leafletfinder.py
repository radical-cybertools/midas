import sys
import numpy as np
import networkx as nx
from pyspark  import  SparkContext
from time import time

def find_edges((vector,counter), size=0, cutoff=15.00):
    size = matrix_size
    frame_list = np.zeros(size, dtype=bool)
    for i in range(counter,size-1):
        if np.sqrt(sum(( vector - coord_matrix_broadcast.value[i+1] )**2))  < cutoff:
            frame_list[i+1] = True

    return frame_list

if __name__=="__main__":


    if len(sys.argv) != 3:
        print "Usage: Leaflet Finder: enter <partitions> <atom_filename>"
        exit(-1)
    else:
        partitions = int(sys.argv[1])
        filename = sys.argv[2]

    start_time = time() 
    sc = SparkContext(appName="PythonLeafletFinder")    
    
    
    coord_matrix = np.load(filename)
    coord_matrix_broadcast = sc.broadcast(coord_matrix)
    matrix_size = len(coord_matrix)
    dist_Matrix = sc.parallelize(coord_matrix,partitions)
    dist_Matrix = dist_Matrix.zipWithIndex()  #key-value pairs
    edge_list = dist_Matrix.map(find_edges)
    adj_matrix = edge_list.collect()

    arr = np.concatenate(adj_matrix).reshape([matrix_size,matrix_size])
    
    #outfile = "adj_matrix"
    #np.save(outfile,arr)
    time_to_create_adj_matrix = time()

    ##
    graph = nx.Graph(arr)
    subgraphs = nx.connected_components(graph)
    indices = [np.sort(g) for g in subgraphs]
    np.save('components.npz.npy',indices)

    print 'Total time to create the Adjacency Matrix: %i sec'  % (time_to_create_adj_matrix - start_time)
    print 'Time to calculate the Connected Components: %i sec ' % (time() - time_to_create_adj_matrix)
    
