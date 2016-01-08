__author__ = "George Chantzialexiou"
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import sys
import numpy as np
from sklearn.metrics.pairwise import euclidean_distances
from datetime import datetime

# ------------------------------------------------------------------------------
#   

if __name__ == '__main__':

    args = sys.argv[1:]
    convergence = bool(sys.argv[1])
    k = int(sys.argv[2])
    DIMENSIONS = int(sys.argv[3])
    CUs = int(sys.argv[4])

    # Aggregate all partial sums of each Cluster  to define the new centroids
    afile = []
    total_sums = []  # total partial sums per cluster
    total_nums = []  # total number of sample samples per cluster
    new_centroids = list()
    read_start = datetime.utcnow()
    # ------ Read centroids ---------#
    centroid = list()
    data = open("centroids.data", "r")   
    read_as_string_array = data.readline().split(',')
    centroid = map(float, read_as_string_array)
    data.close()
    #convert to np array
    centroid = np.asfarray(centroid)
    centroid= np.reshape(centroid,(-1,DIMENSIONS))
    #---------------------------------#


    # initiate values
    for i in range(0,k):
        total_nums.append(0)
        total_sums.append(0)
        new_centroids.append(0)

    for i in range(0,CUs):
        afile.append(open("combiner_file_%d.data" % (i+1), "rb"))
        for line in afile[i]:
            line = line.strip()  #remove newline character
            cluster,p_sum,num = line.split('\t',3)   # split line into cluster No, partial sum and number of partial sums
            cluster = int(cluster)
            p_sum = p_sum.split(',')
            p_sum = map(float,p_sum)
            p_sum = np.asfarray(p_sum)
            if p_sum.shape == (1,):
                p_sum.resize(DIMENSIONS,)
            total_sums[cluster] += p_sum
            total_nums[cluster] += int(num)
        afile[i].close()
    
    read_time = (datetime.utcnow() - read_start).total_seconds()
    exec_start = datetime.utcnow()
    # new values
    convergence = True

    for i in range(0,k):
        if total_nums[i]!=0:
            new_centroids[i] =  total_sums[i] / total_nums[i]

    centroid = np.asfarray(centroid)

    centroid = np.reshape(centroid,(-1,DIMENSIONS))
    arr = np.zeros(DIMENSIONS)

    # check convergence and update centroids
    for i in range(0,k):
        if total_nums[i]!=0 and abs(euclidean_distances(centroid[i],arr) - euclidean_distances(new_centroids[i],arr))>=0.1*euclidean_distances(centroid[i],arr):
            convergence = False
            if total_nums[i]!=0:
                centroid[i] = new_centroids[i]


    exec_time = (datetime.utcnow() - exec_start).total_seconds()
    write_start = datetime.utcnow()
    centroid = np.reshape(centroid,k*DIMENSIONS).tolist()
    #print centroid
    # Put the centroids into a file to share
    centroid_to_string = ','.join(map(str,centroid))
    centroid_file = open('centroids.data', 'w')
    centroid_file.write(centroid_to_string)
    centroid_file.close()
    #--------------------------------------------------------------------------------
    # convergence
    conv = open('converge.txt','w')
    if convergence ==True:
        conv.write('True')
    else:
        conv.write('False')
    conv.close()

    write_time = (datetime.utcnow() - write_start).total_seconds()

    print '{0},{1},{2}'.format(read_time,exec_time,write_time)




    