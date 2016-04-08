#Required package:  GraphFrame 
#http://graphframes.github.io
#Start graphFrame : $SPARK_HOME/bin/spark-shell --packages  graphframes:graphframes:0.1.0-spark1.6

import sys
import numpy as np
import scipy.spatial
from operator import add
from pyspark import  SparkContext
from pyspark.sql import SQLContext
from graphframes import GraphFrame

cutoff = 16.00

def distance(v1,v2);
    if scipy.spatial.distance.euclidean(v1,v2) <cutoff:
        return 1
    else:
        return 0


if __name__="__main__":

    if len(sys.argv) != 2:
        print("Usage: Leaflet Finder <file>", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="PythonLeafletFinder")
    lines = sc.textFile(sys.argv[1], 1)
    no_lines = lines.count()

    vectors = lines.map(lambda line: np.array([float(x) for x in  line.split(' ')]))

    sqlContext = SQLContext(sc)
    vector_list = list()
    
    # create a vector for each point
    for i in xrange(no_lines):
        vect_n = list()
        vect_n.append(i+1)
        vector_list.append(vect_n)

    v = sqlContext.createDataFrame(vector_list, ['id'])
    # v.show()   
    
    edge_list = list()
    for i,val in enumerate(a):
        for j in range(i+1,len(a)):
            if sinartisi(a[i],a[j])==1:
                edge_list.append([i+1,j+1])

    e = sqlContext.createDataFrame(edge_list, ['src', 'dst'])
    e.show()
    
    # create the graph
    g = GraphFrame(v, e)
    #g.vertices.show()
    #g.edges.show()

    cc = g.connectedComponents()
    cc.select("id", "component").orderBy("component").show()




