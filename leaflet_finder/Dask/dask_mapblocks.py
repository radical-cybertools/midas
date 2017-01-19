import sys
import numpy as np
#jimport logging
#logging.basicConfig(stream=sys.stdout, level=logging.CRITICAL)
import random
import os, time, sys, datetime
import sklearn.metrics.pairwise
import scipy.spatial.distance
from scipy.spatial.distance import cdist
import dask
import dask.array as da
import dask.multiprocessing
from dask.diagnostics import ProgressBar
from dask.diagnostics import ResourceProfiler
from dask.dot import dot_graph
from dask.array.core import map_blocks
import dask.bag as db

from distributed import Client, progress



hostname = '198.202.119.139:8786'


#from multiprocessing.pool import ThreadPool
RESULT_DIR="results"
RESULT_FILE_PREFIX="pair-distance-"
HEADER_CSV="Scenario, Type, Time"
#BASE_DIRECTORY=os.getcwd()
# Dask has issues with NFS home directory on Comet
# BASE_DIRECTORY='/scratch/luckow/7146882'
BASE_DIRECTORY='/oasis/scratch/comet/luckow/temp_project'
OUT_DIR=os.path.join(BASE_DIRECTORY, "npy_stack")

FILENAMES=["../132k_dataset/atom_pos_132K.npy", "../145K_dataset/atom_pos_145K.npy", 
          "../300K_dataset/atom_pos_291K.npy", '../840K_dataset/atom_pos_839K.npy']

scenario = FILENAMES[0]

OUTPUT_DIRECTORY="/oasis/scratch/comet/luckow/temp_project/out"

"""Make sure point_array points to correct dataset"""
global point_array
global cutoff

cutoff=15.0


def map_block_distance(block,  block_id=None):
    isCompute = block_id[1]>=block_id[0] and block.shape != (1,1)# Bug ? 
                                         # Dask returns one block with shape (1,1) ID:(1, 1) Shape: (1, 1) Predicate: True Content: [[ 1.]]
    block_length = block.shape[0]
    #logging.debug("ID:" + str(block_id) + " Shape: " + str(block.shape) + \
    #      " Predicate: " + str(isCompute) + " Content: " + str(block) + "\n")
    if isCompute:
        source_start = block_id[0]*block_length
        source_end = (block_id[0]+1)*block_length
        source_points = point_array[source_start:source_end]        
        dest_start = block_id[1]*block_length
        dest_end = (block_id[1]+1)*block_length
        dest_points = point_array[dest_start:dest_end]  
        #logging.debug("Source Idx: %d - %d Dest. Idx: %d - %d\n"%(source_start, source_end, dest_start, dest_end))
        #print "Source Points: " + str(source_points.compute())
        #print "Destination Points: " + str(dest_points.compute())
        return cdist(source_points, dest_points)>cutoff
        #return np.array(block)
    else:
        return np.zeros(block.shape)


def benchmark_dask_map_block(filename, cutoff=15, CHUNKSIZE=4096, number_threads=40, direct_output=True ):
    client = Client(hostname)
    global point_array
    func_name = sys._getframe().f_code.co_name

    results = []
    start = time.time()
    point_array=da.from_npy_stack(filename)
    #print str(point_array.shape)
    end_read = time.time()
    results.append("%s,dask,%s,read_file, %.4f"%(filename, func_name, end_read-start))
    
    dist_matrix = da.ones((point_array.shape[0],point_array.shape[0]), chunks=(CHUNKSIZE, CHUNKSIZE))
    #with ProgressBar():
    
    """map_block_distances operates on point_array """
    out =  dist_matrix.map_blocks(map_block_distance)
    
    # Log performance data
    end_compute = -1
    end_out_write = -1
    outfile = os.path.join(OUTPUT_DIRECTORY, os.path.basename(filename) + "_out.h5")
    #try: 
    #    os.makedirs(outfile) 
    #except: 
    #    pass
    with dask.set_options(get=client.get):
        if direct_output:
            #da.to_npy_stack(outfile, out)    
            out.to_hdf5(outfile, "/o", compression='lzf')
            end_compute = time.time()
            results.append("%s,dask,%s,compute_write, %.4f"%(filename, func_name, end_compute-end_read))
            results.append("%s,dask,%s,total, %.4f"%(filename, func_name, end_compute-start))    
        else:
            out.compute()  
            end_compute = time.time()
            #print "end compute"
            np.save(outfile, out)
            end_out_write = time.time()            
            results.append("%s,dask,%s,compute, %.4f"%(filename, func_name, end_compute-end_read))
            results.append("%s,dask,%s,write_file, %.4f"%(filename, func_name, end_out_write-end_compute))
            results.append("%s,dask,%s,total, %.4f"%(filename, func_name, end_out_write-start))
        
        #os.remove(outfile)
        print("\n".join(results))



#benchmark_dask_map_block('/oasis/scratch/comet/luckow/temp_project/npy_stack/atom_pos_839K.npy_8192')

dask_scenarios = [os.path.abspath(os.path.join(OUT_DIR, i)) for i in os.listdir(OUT_DIR)]
print str(dask_scenarios)


# In[ ]:

for s in dask_scenarios:
    if "_8192" in s and '839K' not in s:
        print "Process: %s"%s
        benchmark_dask_map_block(s)


