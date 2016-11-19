#!/usr/bin/env python

import sys, getopt,ast
import numpy as np
import argparse
from time import time

def dH((P, Q)):
    def vsqnorm(v, axis=None):
        return np.sum(v*v, axis=axis)
    Ni = 3./P.shape[1]
    d = np.array([vsqnorm(pt - Q, axis=1) for pt in P])
    return ( max(d.min(axis=0).max(), d.min(axis=1).max())*Ni )**0.5


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("element_set1", help="The first Set of trajectories that will be used")
    parser.add_argument("element_set2", help="The second set of trajectories that will be used")
    parser.add_argument("sel", help="Enter aa, ha, or ca for atom selection")
    parser.add_argument("size", help="Trajectories length. Valid Values: short,med,long. Default Value: short")
    args = parser.parse_args()
    
    set1 = ast.literal_eval(args.element_set1)
    set2 = ast.literal_eval(args.element_set2)
    sel = args.sel
    size = args.size
    start_time = time()
    if size == 'med':
        trj_list1 = [np.hstack( ( np.load('trj_%s_%03i.npz.npy' % (sel, i)),    \
                                 np.load('trj_%s_%03i.npz.npy' % (sel, i)) ) ) \
                                 for i in set1]
    elif size == 'long':
        trj_list1 = [np.hstack( ( np.load('trj_%s_%03i.npz.npy' % (sel, i)),    \
                                 np.load('trj_%s_%03i.npz.npy' % (sel, i)),    \
                                 np.load('trj_%s_%03i.npz.npy' % (sel, i)),    \
                                 np.load('trj_%s_%03i.npz.npy' % (sel, i)) ) ) \
                                 for i in set1]
    else:
        trj_list1 = [np.load('trj_%s_%03i.npz.npy' % (sel, i)) for i in set1]

    if set2 != set1:
        if size == 'med':
            trj_list2 = [np.hstack( ( np.load('trj_%s_%03i.npz.npy' % (sel, i)),    \
                                     np.load('trj_%s_%03i.npz.npy' % (sel, i)) ) ) \
                                     for i in set2]
        elif size == 'long':
            trj_list2 = [np.hstack( ( np.load('trj_%s_%03i.npz.npy' % (sel, i)),    \
                                     np.load('trj_%s_%03i.npz.npy' % (sel, i)),    \
                                     np.load('trj_%s_%03i.npz.npy' % (sel, i)),    \
                                     np.load('trj_%s_%03i.npz.npy' % (sel, i)) ) ) \
                                     for i in set2]
        else:
            trj_list2 = [np.load('trj_%s_%03i.npz.npy' % (sel, i)) for i in set2]
    else:
        trj_list2 = trj_list1

    data_init = time()

    comp = np.zeros((len(set1),len(set2)))

    for i in range(1,len(set1)+1):
        for j in range(1,len(set2)+1):
            comp[i-1,j-1]=dH((trj_list1[i-1],trj_list2[j-1]))
    exec_time = time()     
    np.save('distances.npz.npy',comp)
    total_time = time()
    print 'Data Read: %f sec, Exec: %f'%(data_init-start_time,exec_time-data_init)
    print 'Total Time of execution is : %f sec ' % (total_time - start_time)

