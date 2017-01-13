import sys
import numpy as np
from pyspark import SparkContext
from time import time
import networkx as nx





def find_partial_connected_components(data,cutoff=15.00):
    
    window = data[0]
    i_index = data[1][0]
    j_index = data[1][1]
    import numpy as np 
    import networkx as nx
    graph = nx.Graph()

    ## pairwise distances
    for i in range(0,len(window[0])):
        for j in range(0,len(window[1])):
            if np.sqrt(sum((window[0][i] - window[1][j]) ** 2)) <= cutoff:
                graph.add_edge(i+i_index,j+j_index)    # fix indexes
                
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
        part_size = int(sys.argv[1])
        filename = sys.argv[2]

    start_time = time() 
    sc = SparkContext(appName="PythonLeafletFinder")    

    coord_matrix = np.load(filename)
    matrix_size = coord_matrix.shape[0] 

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
    connected_components = dist_Matrix.flatMap(find_partial_connected_components).reduce(merge_spanning_trees)

    stop_time = time()
    indices = [np.sort(list(g)) for g in connected_components]
    np.save('components.npz.npy',indices)
    total_time = stop_time - start_time
    total_time = total_time/60

    print 'Total time of completion is: %d minutes'  % (total_time)










