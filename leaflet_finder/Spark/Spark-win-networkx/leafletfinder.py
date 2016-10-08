import sys
import numpy as np
import networkx as nx
from pyspark import SparkContext
from time import time

"""
## Algorithm
credit to https://www.cs.rice.edu/~vs3/comp422/lecture-notes/comp422-lec24-s08-v2.pdf  page 38


- partition the adjacency matrix across processors
- run indepentent connected components on each processor (find_partial_connected_components)
- merge spanning forests until one remains  (unify_components)

For Merging:
I use intersection and union functions:
- intersection: I check if there is at least one common node, which means MSTs are connected
- union: if there is a common node, I do union and add to the set of connected componnents, otherwise I add each node alone.

I run the merging function pairwise, and repeat till only one list remain.

"""

def find_partial_connected_components(data,cutoff=15.00):
    
    window = data[0]
    i_index = data[1][0]
    j_index = data[1][1]
    
    import networkx as nx
    graph = nx.Graph()
    
    for i in range(0,len(window[0])):
        for j in range(0,len(window[1])):
            if np.sqrt(sum((window[0][i] - window[1][j]) ** 2)) <= cutoff:
                graph.add_edge(i+i_index,j+j_index)    # fix indexes

    connected_components = nx.connected_components(graph)
    cc = []
    for x in connected_components:
        cc.append(x)

    return  cc


def unify_components(cc1,cc2):
    unified_componnents = []
    for x in cc1:
        for y in cc2:
            if len(x.intersection(y))!=0:
                union = x.union(y)
                if not union in unified_componnents:
                    unified_componnents.append(x.union(y))
            else:
                if not x in unified_componnents:
                    unified_componnents.append(x)
                if not y in unified_componnents:
                    unified_componnents.append(y)

    return unified_componnents


if __name__=="__main__":


    if len(sys.argv) != 3:
        print "Usage: Leaflet Finder: enter <partition_size> <atom_filename>"
        exit(-1)
    else:
        part_size = int(sys.argv[1])
        filename = sys.argv[2]

    start_time = time() 
    sc = SparkContext(appName="PythonLeafletFinder")    
    
    
    coord_matrix = np.load(filename)
    matrix_size = coord_matrix.shape[0]  # Ask yiannis if this is the matrix size, cause it was accidentaly deleted

    arranged_coord = list()
    for i in range(1,matrix_size+1,part_size):
        for j in range(i,matrix_size,part_size):
            # The arranged_elem contains a tuple with the data needed to calculate
            # in a window. The first part of the tuple is a list that contains
            # two numpy arrays. The second element has indices of the first element
            # of both arrays.
            arranged_elem = ([coord_matrix[i-1:i-1+part_size],coord_matrix[j-1:j-1+part_size]],[i,j])
            arranged_coord.append(arranged_elem)


    print len(arranged_coord)
    dist_Matrix = sc.parallelize(arranged_coord,len(arranged_coord))

    # if this RDD is use in a function keep in mind that the pair
    # (value,index) will be passed. As a result it is collected
    # with the index and becomes a list of pairs.
    # From spark docs:
    # >>> sc.parallelize(["a", "b", "c", "d"], 3).zipWithIndex().collect()
    # [('a', 0), ('b', 1), ('c', 2), ('d', 3)]


    connected_components = dist_Matrix.map(find_partial_connected_components).reduce(unify_components) 

    

    indices = [np.sort(list(g)) for g in connected_components]
    np.save('components.npz.npy',indices)

    stop_time = time()


    print 'Total Time of execution is : %i sec ' % (stop_time - start_time)
    
