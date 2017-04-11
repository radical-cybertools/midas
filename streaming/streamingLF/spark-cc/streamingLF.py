import sys
import time
import numpy as np
import networkx as nx
from pyspark  import  SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import  KafkaUtils

####### CONFIGURATIONS  #########
METABROKER_LIST = sys.argv[1]
topic = sys.argv[2]
number_of_executors = 2
STREAMING_WINDOW = 60
number_of_partitions = 24
##################################
def find_partial_connected_components(data,cutoff=15.00):
    
    import networkx as nx
    from scipy import import import spatial
    import numpy as np
    window = data[0]
    i_index = data[1][0]
    j_index = data[1][1]
    
    ## pairwise distances
    graph = nx.Graph()
    distances = spatial.distance.cdist(window[0],window[1])<=cutoff  # check indexes
    for i in range(0,len(window[0])):
        for j in range(0,len(window[1])):
            if distances[i][j]:
                graph.add_edge(i+i_index,j+j_index)    # fix indexes
    
    # partial connected components
    connected_components = nx.connected_components(graph)
    for x in connected_components:
        yield [x]                                                                                                                    yield [x]

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




ssc_start = time.time()
sc = SparkContext(appName="LeafletFinderStreaming")
ssc = StreamingContext(sc, STREAMING_WINDOW)
kafka_dstream = KafkaUtils.createDirectStream(ssc, [TOPIC], {"metadata.broker.list": METABROKER_LIST })
ssc_end = time.time()



