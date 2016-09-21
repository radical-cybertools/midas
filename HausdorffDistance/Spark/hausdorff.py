import sys
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
    
    P = data[0][0]
    Q = data[0][1]

    i_traj = data[1][0]
    j_traj = data[1][1]

    dist=np.zeros((len(P),len(Q)))

    for i in range(0,len(P)):
        for j in range(0,len(Q)):
          dist[i,j]=dH(P[i],Q[j])
    
    return (dist,[i_traj,j_traj])


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
    sc = SparkContext(appName="PythonHausdorffDistance")    
    
    trajectories = list()
    if Size == 'long':
        for i in range(1,NumOfTrj+1):
            trajectories.append(np.hstack(np.load('trj_%s_%03i.npz.npy'%(Sel,i)), \
                                          np.load('trj_%s_%03i.npz.npy'%(Sel,i)), \
                                          np.load('trj_%s_%03i.npz.npy'%(Sel,i)), \
                                          np.load('trj_%s_%03i.npz.npy'%(Sel,i))))
    elif Size == 'med':
        for i in range(1,NumOfTrj+1):
            trajectories.append(np.hstack(np.load('trj_%s_%03i.npz.npy'%(Sel,i)), \
                                          np.load('trj_%s_%03i.npz.npy'%(Sel,i))))
    else:
        for i in range(1,NumOfTrj+1):
            trajectories.append(np.load('trj_%s_%03i.npz.npy'%(Sel,i)))

    arranged_traj = list()
    for i in range(1,NumOfTrj+1,WindowSize):
        for j in range(i,NumOfTrj,WindowSize):
            # The arranged_elem contains a tuple with the data needed to calculate
            # in a window. The first part of the tuple is a list that contains
            # two numpy arrays. The second element has indices of the first element
            # of both arrays.
            arranged_elem = ([trajectories[i-1:i-1+WindowSize],trajectories[j-1:j-1+WindowSize]],[i,j])
            arranged_traj.append(arranged_elem)


    print arranged_traj
    traj_par = sc.parallelize(arranged_traj,len(arranged_traj))

    # if this RDD is use in a function keep in mind that the pair
    # (value,index) will be passed. As a result it is collected
    # with the index and becomes a list of pairs.
    # From spark docs:
    # >>> sc.parallelize(["a", "b", "c", "d"], 3).zipWithIndex().collect()
    # [('a', 0), ('b', 1), ('c', 2), ('d', 3)]

    dist = traj_par.map(HausdorffDist)

    dist_Matrix = np.zeros((NumOfTrj,NumOfTrj), dtype=float)
    for element in dist.collect():
        print element[1][0],element[1][1], element[0]
        dist_Matrix[element[1][0]-1:element[1][0]-1+WindowSize,element[1][1]-1:element[1][1]-1+WindowSize] = element[0]

    np.save('hausdorff_distances.npz.npy',dist_Matrix)

    stop_time = time()


    print 'Total Time of execution is : %i sec ' % (stop_time - start_time)
    
