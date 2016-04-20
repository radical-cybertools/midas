#Required package:  GraphFrame                                                                              
#http://graphframes.github.io
#Start graphFrame : $SPARK_HOME/bin/pyspark --packages graphframes:graphframes:0.1.0-spark1.6

import sys
import numpy as np
from pyspark  import  SparkContext
from pyspark.sql  import SQLContext, Row
from graphframes import GraphFrame
from time import time

#NUMBER_OF_EXECUTORS = 2
def find_edges((vector,counter), size=0, cutoff=256.00):
    size = matrix_size
    frame_list = list()
    for i in range(counter,size-1):
        if sum(( vector - coord_matrix_broadcast.value[i+1] )**2)  < cutoff:
            frame_list.append([counter+1,i+2])
    if frame_list:
        return frame_list
    else:
        return [[-1]]

if __name__=="__main__":


    if len(sys.argv) != 2:
        print "Usage: Leaflet Finder <file>"
        exit(-1)
    start_time = time() 
    sc = SparkContext(appName="PythonLeafletFinder")    
    
    filename = str(sys.argv[1])
    #filename = '/home/user/leaflet-spark/atom_position_frame_1.npz.npy'
    
    coord_matrix = np.load(filename)
    coord_matrix_broadcast = sc.broadcast(coord_matrix)
    matrix_size = len(coord_matrix)
    dist_Matrix = sc.parallelize(coord_matrix)
    dist_Matrix = dist_Matrix.zipWithIndex()  #key-value pairs
    edge_list = dist_Matrix.flatMap(find_edges)
    
    edge_list = edge_list.filter(lambda x: x[0]!=-1) # filter the -1 values
    
    sqlContext = SQLContext(sc)
    
    Edges = Row('src','dst')
    edge = edge_list.map(lambda x: Edges(*x))
    e = sqlContext.createDataFrame(edge)
    # e.take(10)
    v = sqlContext.createDataFrame(sc.parallelize(xrange(matrix_size)).map(lambda i:Row(id=i+1)))
    # v.show()
    
    # create the graph
    g = GraphFrame(v, e)
    #g.vertices.show()
    #g.edges.show()
    total_time = time() - start_time
    cc = g.connectedComponents()
    print cc.select("id", "component").orderBy("component").show()
    print 'Total time to create the Graphframe: %i sec'  % (total_time)
    print 'Time to calculate the connected components: %i sec ' % (time() - total_time-start_time)
    
