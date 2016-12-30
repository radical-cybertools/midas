#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import numpy as np
from time import time
import argparse


def get_distance(Atom1, Atom2):
    # Calculate Euclidean distance. 1-D and 3-D in the future
    return np.sqrt(sum((Atom1 - Atom2) ** 2))


if __name__ == '__main__':


    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="File with the atoms positions")
    parser.add_argument("win_size", help="Dimension size of the submatrix")
    parser.add_argument("i_coord", help="i coordinate at the distance matrix")
    parser.add_argument("j_coord", help="j coordinate at the distance matrix")
    parser.add_argument("cutoff", help="The cutoff value for the edge existence")
    args = parser.parse_args()

    filename = args.file
    WINDOW_SIZE = int(args.win_size)
    reading_start_point_i = int(args.i_coord) -1
    j_dim = int(args.j_coord) -1
    cutoff = float(args.cutoff)

    #----------------------Reading Input File-------------------------------#
    start = time()

    atoms = np.load(filename)

    data_init = time()
    # the difference is that in the Cus compute data that are in main diagonal compute half of the elements 
    # because table is symmetric, so the second loop can be half in the first case 
    distances=np.empty((WINDOW_SIZE,WINDOW_SIZE),dtype='bool')
    if reading_start_point_i == j_dim:
        for i in range(0,WINDOW_SIZE):
            for j in range(i+1,WINDOW_SIZE):
                dist = get_distance(atoms[reading_start_point_i+i],atoms[reading_start_point_i+j])  
                if dist<=cutoff:
                    distances[i][j]=True 
                else:
                    distances[i][j]=False
    else:
        for i in range(0,WINDOW_SIZE):
            for j in range(0,WINDOW_SIZE):
                dist = get_distance(atoms[reading_start_point_i+i],atoms[j_dim+j])  
                if dist<=cutoff:
                    distances[i][j]=True
                else:
                    distances[i][j]=False
    exec_time = time()
    np.save("distances_%d_%d.npz.npy" % (reading_start_point_i,j_dim),distances)

    write_time = time()
    print '%f,%f,%f'%(data_init - start, exec_time - data_init, write_time - exec_time)
