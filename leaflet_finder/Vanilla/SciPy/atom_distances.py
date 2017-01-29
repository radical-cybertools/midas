#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import numpy as np
from time import time

if __name__ == '__main__':

    from scipy.spatial.distance import cdist

    parser = argparse.ArgumentParser()
    parser.add_argument("file", help="File with the atoms positions")
    parser.add_argument("i_coord", help="i coordinate at the distance matrix")
    parser.add_argument("j_coord", help="j coordinate at the distance matrix")
    parser.add_argument("cutoff", help="The cutoff value for the edge existence")
    args = parser.parse_args()

    filename = args.file
    reading_start_point_i = int(args.i_coord) -1
    j_dim = int(args.j_coord) -1
    cutoff = float(args.cutoff)

    #----------------------Reading Input File-------------------------------#
    start = time()

    atoms = np.load(filename)

    data_init = time()
    
    distances = (cdist(atoms[reading_start_point_i+i],atoms[reading_start_point_i+j])<cutoff)
    
    exec_time = time()
    np.save("distances_%d_%d.npz.npy" % (reading_start_point_i,j_dim),distances)

    write_time = time()
    print '%f,%f,%f'%(data_init - start, exec_time - data_init, write_time - exec_time)
