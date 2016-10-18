#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import math
import numpy as np


def get_distance(Atom1, Atom2):
    # Calculate Euclidean distance. 1-D and 3-D in the future
    return np.sqrt(sum((Atom1 - Atom2) ** 2))

def n_dim_input_to_numpy_array(temp):
    temp = temp.split(',')
    temp = map(float,temp)
    return np.asfarray(temp)

if __name__ == '__main__':


    args = sys.argv[1:]
    x_start = int(sys.argv[2])
    window_size = int(sys.argv[1])
    total_file_lines =  int(sys.argv[4])
    cutoff = float(sys.argv[5])

    #----------------------Reading Input File-------------------------------#

    read_file = open('input.txt')

    calc_count = 0 
    outer_loop = 0
    inner_loop_if = 0
    inner_loop_else = 0

    atoms = list()
    for line in read_file:
        atoms.append(n_dim_input_to_numpy_array(line))

    if (x_start+1)*window_size>total_file_lines:
        distances = np.empty((total_file_lines%window_size,total_file_lines),dtype='bool')
    else:
        distances=np.empty((window_size,total_file_lines),dtype='bool')
    for countx in enumerate(atoms):
        outer_loop += 1
        if countx[0] >=window_size*x_start and countx[0] <window_size*(x_start+1):
            inner_loop_if  += 1
            for county in enumerate(atoms):
                dist_temp = get_distance(atoms[countx[0]],atoms[county[0]])
                calc_count += 1
                if dist_temp<=cutoff:
                    distances[countx[0]%window_size][county[0]] = True
                else:
                    distances[countx[0]%window_size][county[0]] = False
        inner_loop_else += 1
    

    np.save("distances_%d.npz.npy" % (x_start),distances)
    print 'outer loop, inner loop if, inner loop else, calcs: {0}, {1}, {2}, {3}'.format(str(outer_loop), str(inner_loop_if), str(inner_loop_else), str(calc_count))