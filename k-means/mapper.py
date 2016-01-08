__author__ = "George Chantzialexiou"
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

from sklearn.metrics.pairwise import euclidean_distances
import sys
import numpy as np
from datetime import datetime


################################################################################
##
if __name__ == "__main__":

    args = sys.argv[1:]

    curent_cu = int(sys.argv[1])
    k = int(sys.argv[2])  #number of clusters
    chunk_size = int(sys.argv[3])
    total_CUs = int(sys.argv[4])
    DIMENSIONS = int(sys.argv[5])
    offset_start = int(sys.argv[6])
    offset_end = int(sys.argv[7])

    #----------------------Reading the Centroid files-------------------------------
    read_start = datetime.utcnow()
    centroid = list()
    data = open("centroids.data", "r")   
    read_as_string_array = data.readline().split(',')
    centroid = map(float, read_as_string_array)
    data.close()
    #convert to np array
    centroid = np.asfarray(centroid)
    centroid= np.reshape(centroid,(-1,DIMENSIONS))

    #-----------------Reading the Elements file --------------
    read_file = open('dataset.in', 'rb')
    read_file.seek(offset_start) # start reading from here

    elements = list()
    # this is the sign of EOF. last CU compute until the EOF
    if curent_cu == total_CUs:
        for i, line in enumerate(read_file):
            if line == "":
                break
            elements.append(line)
    else:
        # we only read the elements we will process - Each CU compute a part of the file
        while read_file.tell()!=offset_end:
            line = read_file.readline()
            elements.append(line)
    
    read_file.close()
    elements = map(float, elements)
    #convert to np array
    elements = np.asfarray(elements)
    elements = np.reshape(elements,(-1,DIMENSIONS))
    read_time = (datetime.utcnow() - read_start).total_seconds()
    
    #----------------------------------------------------------------------------------
    exec_start = datetime.utcnow()
    sum_elements_per_centroid = list()  # partial sum of cluster's sample in this task
    num_elements_per_centroid = list()  # number of samples of each cluster in the same map task.
    for i in range(0,k):
        sum_elements_per_centroid.append(0)
        num_elements_per_centroid.append(0)

    for i in range(0,len(elements)):
        minDist = euclidean_distances(elements[i], centroid[0])  # sklearn.metrics distance
        cluster = 0
        for j in range(1,k):
            curDist = euclidean_distances(elements[i], centroid[j])
            if minDist != min(minDist,curDist):
                cluster = j  # closest centroid is centroid No: j
                minDist = curDist
        sum_elements_per_centroid[cluster] += elements[i]
        num_elements_per_centroid[cluster] += 1
        
    exec_time = (datetime.utcnow() - exec_start).total_seconds()
    write_start = datetime.utcnow()
    # Write results into a file
    combiner_file = open("combiner_file_%d.data" % curent_cu, "w")
    for cluster in range(0,k):
        sum_elements_per_centroid_list = np.array(sum_elements_per_centroid[cluster]).tolist()
        if sum_elements_per_centroid_list!=0:
            sum_elements_per_centroid_list = ','.join(map(str,sum_elements_per_centroid_list))
        string = '%s\t%s\t%s\n' % (cluster, sum_elements_per_centroid_list,num_elements_per_centroid[cluster])
        combiner_file.write(string) 
    combiner_file.close()
    write_time = (datetime.utcnow()-write_start).total_seconds()

    print '{0},{1},{2}'.format(read_time,exec_time,write_time)