#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import numpy as np

def get_distance(Atom1, Atom2):
    # Calculate Euclidean distance. 1-D and 3-D in the future
    return np.sqrt(sum((Atom1 - Atom2) ** 2))

def n_dim_input_to_numpy_array(temp):
    temp = temp.split(',')
    temp = map(float,temp)
    return np.asfarray(temp)
    calc_count = calc_count +1

if __name__ == '__main__':


    args = sys.argv[1:]
    WINDOW_SIZE = int(sys.argv[1])
    reading_start_point_i = int(sys.argv[2]) -1
    j_dim = int(sys.argv[3]) -1
    total_file_lines =  int(sys.argv[4])
    cutoff = float(sys.argv[5])

    #----------------------Reading Input File-------------------------------#

    read_file = open('input.txt')


    atoms = list()
    for count, line in enumerate(read_file):
        if count == total_file_lines+1 or count >= reading_start_point_i+WINDOW_SIZE:
            break
        if count >= reading_start_point_i and count <reading_start_point_i+WINDOW_SIZE:
            atoms.append(n_dim_input_to_numpy_array(line))

    # That means that we are not calculating the elements of the main diagonal which are the same.
    # We do calculate differnt atoms
    if reading_start_point_i!=j_dim:
        atomsY = list()
        atomsY.append(n_dim_input_to_numpy_array(line)) # already read from previous for-loop
        for countY, line in enumerate(read_file):
            if countY > j_dim + WINDOW_SIZE-count-1:
                break
            if countY >= j_dim-count and countY < j_dim + WINDOW_SIZE-count-1:  #-1 because we already appended the first line 
                atomsY.append(n_dim_input_to_numpy_array(line))
    read_file.close()

    # the difference is that in the Cus compute data that are in main diagonal compute half of the elements 
    # because table is symmetric, so the second loop can be half in the first case 
    distances=np.empty((WINDOW_SIZE,WINDOW_SIZE),dtype='bool')
    if reading_start_point_i == j_dim:
        for i in range(0,WINDOW_SIZE):
            for j in range(i+1,WINDOW_SIZE):
                dist = get_distance(atoms[i],atoms[j])  
                if dist<=cutoff:
                    distances[i][j]=True 
                else:
                    distances[i][j]=False
    else:
        for i in range(0,WINDOW_SIZE):
            for j in range(0,WINDOW_SIZE):
                dist = get_distance(atoms[i],atomsY[j])  
                if dist<=cutoff:
                    distances[i][j]=True
                else:
                    distances[i][j]=False

    np.save("distances_%d_%d.npz.npy" % (reading_start_point_i,j_dim),distances)

