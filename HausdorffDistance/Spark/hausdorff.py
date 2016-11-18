import sys
import os
import numpy as np
from pyspark import SparkContext
from time import time

def HausdorffDist(data):

    def dH(P, Q):
        def vsqnorm(v, axis=None):
            return np.sum(v*v, axis=axis)
        Ni = 3./P.shape[1]
        d = np.array([vsqnorm(pt - Q, axis=1) for pt in P])
        return ( max(d.min(axis=0).max(), d.min(axis=1).max())*Ni )**0.5

    task_start = time()

    P = data[0][0]
    Q = data[0][1]

    P_list = list()
    for filename in P:
        if TrajSize == 'long':
            P_list.append(np.hstack((np.load(TrajSize), \
                                     np.load(TrajSize), \
                                     np.load(TrajSize), \
                                     np.load(TrajSize))))
        elif TrajSize == 'med':
            P_list.append(np.hstack((np.load(TrajSize), \
                                     np.load(TrajSize))))
        else:
            P_list.append(np.load(TrajSize))

    Q_list = list()
    for filename in Q:
        if TrajSize == 'long':
            Q_list.append(np.hstack((np.load(TrajSize), \
                                     np.load(TrajSize), \
                                     np.load(TrajSize), \
                                     np.load(TrajSize))))
        elif TrajSize == 'med':
            Q_list.append(np.hstack((np.load(TrajSize), \
                                     np.load(TrajSize))))
        else:
            Q_list.append(np.load(TrajSize))
    
    data_read = time()

    dist=np.zeros((len(P_list),len(Q_list)))

    for i in range(0,len(P_list)):
        for j in range(0,len(Q_list)):
          dist[i,j]=dH(P_list[i],Q_list[j])
    
    exec_time = time()

    return (dist,[data[1][0],data[1][1]],task_start,data_read,exec_time)


if __name__=="__main__":


    if len(sys.argv) != 5:
        print "Usage: hausdorff.py <NumOfTrj> <Sel> <Size> <WindowSize>"
        exit(-1)
    else:
        NumOfTrj = int(sys.argv[1])
        Sel = sys.argv[2]
        Size = sys.argv[3]
        WindowSize = int(sys.argv[4])

    start_time = time()   

    trajectories = ['trj_%s_%03i.npz.npy'%(Sel,i) for i in range(1,NumOfTrj+1)]
    
    arranged_traj = list()
    for i in range(1,NumOfTrj+1,WindowSize):
        for j in range(i,NumOfTrj,WindowSize):
            # The arranged_elem contains a tuple with the data needed to calculate
            # in a window. The first part of the tuple is a list that contains
            # two numpy arrays. The second element has indices of the first element
            # of both arrays.
            arranged_elem = ([trajectories[i-1:i-1+WindowSize],trajectories[j-1:j-1+WindowSize]],[i,j])
            arranged_traj.append(arranged_elem)

    data_init = time()
    
    sc = SparkContext(appName="PythonHausdorffDistance")

    TrajSize = sc.broadcast(Size)

   
    traj_par = sc.parallelize(arranged_traj,len(arranged_traj))

    # if this RDD is use in a function keep in mind that the pair
    # (value,index) will be passed. As a result it is collected
    # with the index and becomes a list of pairs.
    # From spark docs:
    # >>> sc.parallelize(["a", "b", "c", "d"], 3).zipWithIndex().collect()
    # [('a', 0), ('b', 1), ('c', 2), ('d', 3)]

    dist = traj_par.map(HausdorffDist)
    dist_Matrix = np.zeros((NumOfTrj,NumOfTrj), dtype=float)
    timing = np.empty(shape=(0,3),dtype=float)

    for element in dist.collect():
        dist_Matrix[element[1][0]-1:element[1][0]-1+WindowSize,element[1][1]-1:element[1][1]-1+WindowSize] = element[0]
        timing = np.vstack((timing,np.array([element[2],element[3],element[4]])))
    
    write_time = time()
    np.save('hausdorff_distances.npz.npy',dist_Matrix)

    stop_time = time()
    np.save('timing.npz.npy',timing)
    print 'start,%d,dataInit,%d,write,%d,stop,%d'%(start_time*1000,data_init*1000,write_time*1000,stop_time*1000)
    

