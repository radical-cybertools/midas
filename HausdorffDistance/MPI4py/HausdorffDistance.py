import sys
import os
import argparse
import numpy as np
from time import time
from mpi4py import MPI


def HausdorffDist(data):

    def dH(P, Q):
        def vsqnorm(v, axis=None):
            return np.sum(v * v, axis=axis)
        Ni = 3. / P.shape[1]
        d = np.array([vsqnorm(pt - Q, axis=1) for pt in P])
        return (max(d.min(axis=0).max(), d.min(axis=1).max()) * Ni)**0.5

    task_start = time()

    P = data[0][0]
    Q = data[0][1]

    P_list = list()
    for filename in P:
        if TrajSize == 'long':
            P_list.append(np.hstack((np.load(TrajSize),
                                     np.load(TrajSize),
                                     np.load(TrajSize),
                                     np.load(TrajSize))))
        elif TrajSize == 'med':
            P_list.append(np.hstack((np.load(TrajSize),
                                     np.load(TrajSize))))
        else:
            P_list.append(np.load(TrajSize))

    Q_list = list()
    for filename in Q:
        if TrajSize == 'long':
            Q_list.append(np.hstack((np.load(TrajSize),
                                     np.load(TrajSize),
                                     np.load(TrajSize),
                                     np.load(TrajSize))))
        elif TrajSize == 'med':
            Q_list.append(np.hstack((np.load(TrajSize),
                                     np.load(TrajSize))))
        else:
            Q_list.append(np.load(TrajSize))

    data_read = time()

    dist = np.zeros((len(P_list), len(Q_list)))

    for i in range(0, len(P_list)):
        for j in range(0, len(Q_list)):
            dist[i, j] = dH(P_list[i], Q_list[j])

    exec_time = time()

    return (dist, [data[1][0], data[1][1]], task_start, data_read, exec_time)


#-----------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("trajNum", help="Number of trajectories")
    parser.add_argument("size", help="Trajectory size")
    parser.add_argument('datafolder', help='Folder the trajectories are')
    args = parser.parse_args()
    trajNum = int(args.trajNum)
    trajSize = int(args.size)
    datafolder = args.datafolder

    start_time = MPI.Wtime()
    comm = MPI.COMM_WORLD
    world_size = comm.Get_size()
    proc_rank = comm.Get_rank()

    window = np.int(np.div(trajNum, np.sqrt(world_size)))

    trajectories = [datafolder + '/trj_aa_%03i.npz.npy' %
                    i for i in range(1, NumOfTrj + 1)]

    arranged_traj = list()
    for i in range(1,trajNum+1,window):
        for j in range(1,trajNum,window):
            # The arranged_elem contains a tuple with the data needed to calculate
            # in a window. The first part of the tuple is a list that contains
            # two numpy arrays. The second element has indices of the first element
            # of both arrays.
            arranged_elem = ([trajectories[i-1:i-1+window],trajectories[j-1:j-1+window]])
            arranged_traj.append(arranged_elem)

    comp_start_time = MPI.Wtime()

    temp_comp = HausdorffDist(arranged_traj[proc_rank])
    comp_end_time = MPI.Wtime()

    np.save(os.getcwd() + '/distances_{}_{}_{}.npz.npy'.format(trajSize,proc_rank),temp_comp)

    write_res = MPI.Wtime()

    # All processes write profile files including all compute times for each
    # task.
    prof_file = open('profile_rank_%d.txt' % proc_rank, 'w')
    prof_file.write('%f\n' % (start_time))
    prof_file.write('%f\n' % (comp_start_time))
    prof_file.write('%f\n' % (comp_end_time))
    prof_file.write('%f\n' % (write_res))
