import sys
import numpy as np
from pyspark import SparkContext
from sklearn.neighbors import NearestNeighbors, BallTree, KDTree
from time import time
import networkx as nx



def find_partial_connected_components(data,cutoff=15.0):

    import networkx as nx
    import numpy as np
    from sklearn.neighbors import BallTree
    
    tree = BallTree(data, leaf_size=40)
    edges = tree.query_radius(data, cutoff)
    edge_list=[list(zip(np.repeat(idx, len(dest_list)), \
            dest_list)) for idx, dest_list in enumerate(edges)]

    edge_list_flat = np.array([list(item) \
            for sublist in edge_list for item in sublist])
    res = edge_list_flat
    res_tree = edge_list_flat[edge_list_flat[:,0]<edge_list_flat[:,1], :]

    graph =nx.from_edgelist(res_tree)

    # partial connected components

    connected_components = nx.connected_components(graph)
    for x in connected_components:
        yield [x]



def merge_spanning_trees(c1,c2):
    while len(c2)!=0:
        temp = c2[0]
        c2.pop(0)
        for i,item in enumerate(c1):
            if item.intersection(temp):
                temp.union(item)
                c1.pop(i)
        c1.append(temp)
    
    return c1


if __name__=="__main__":


    if len(sys.argv) != 3:
        print "Usage: Leaflet Finder: enter <partition_size> <atom_filename>"
        exit(-1)
    else:
        partition_length = int(sys.argv[1])
        filename = sys.argv[2]

    start_time = time() 
    sc = SparkContext(appName="PythonLeafletFinder")

    coord_matrix = np.load(filename)
    matrix_size = coord_matrix.shape[0]
    #partition_length = matrix_size/20   # Fix this: task size

    arranged_coord = list()
    for i in range(0,matrix_size,partition_length):
        arranged_elem = coord_matrix[i:i+partition_length,:]
        arranged_coord.append(arranged_elem)

    dist_Matrix = sc.parallelize(arranged_coord,len(arranged_coord))
    connected_components = dist_Matrix.flatMap(find_partial_connected_components).reduce(merge_spanning_trees)

    stop_time = time()
    indices = [np.sort(list(g)) for g in connected_components]
    np.save('components.npz.npy',indices)
    total_time = stop_time - start_time
    total_time_min = total_time/60

    print 'Total time of completion is: %d minutes'  % (total_time_min)
    print 'Total time of completion is: %0.2f seconds' % (total_time)

