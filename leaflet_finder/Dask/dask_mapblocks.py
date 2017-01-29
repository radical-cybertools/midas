import sys
import numpy as np
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
import dask.dataframe as df
import random
import logging
logging.basicConfig(stream=sys.stdout, level=logging.CRITICAL)
from chest import Chest
from random import shuffle
import os.path
from distributed import Client

hostname="198.202.115.240:8786"
client = Client(hostname)

#from multiprocessing.pool import ThreadPool
RESULT_DIR="results"
RESULT_FILE_PREFIX="pair-distance-"
HEADER_CSV="Scenario, Type, Time"
#BASE_DIRECTORY=os.getcwd()
# Dask has issues with NFS home directory on Comet
# BASE_DIRECTORY='/scratch/luckow/7146882'
BASE_DIRECTORY='/oasis/scratch/comet/luckow/temp_project'
#BASE_DIRECTORY='/scratch/luckow/7218009/'
OUT_DIR=os.path.join(BASE_DIRECTORY, "npy_stack")

FILENAMES=["../132k_dataset/atom_pos_132K.npy", "../145K_dataset/atom_pos_145K.npy", 
          "../300K_dataset/atom_pos_291K.npy", '../840K_dataset/atom_pos_839K.npy']

scenario = FILENAMES[0]



"""Make sure point_array points to correct dataset"""
global point_array
global cutoff

cutoff=15.0

def map_blocks_1d_sparse(block, block_id):
    #new_block = block[:, :, None]
    
    isCompute = block_id[1]>=block_id[0] and block.shape != (1,1)# Bug ? 
                                         # Dask returns one block with shape (1,1) ID:(1, 1) Shape: (1, 1) Predicate: True Content: [[ 1.]]
    block_length = block.shape[0]
    logging.debug("ID:" + str(block_id) + " Shape: " + str(block.shape) +           " Predicate: " + str(isCompute) + " Content: " + str(block) + "\n")
    
    source_start = block_id[0]*block_length
    source_end = (block_id[0]+1)*block_length
    source_points = point_array[source_start:source_end]        
    dest_start = 0
    dest_end = block.shape[1]
    dest_points = point_array[dest_start:dest_end]  
    logging.debug("Source Idx: %d - %d Dest. Idx: %d - %d\n"%(source_start, source_end, dest_start, dest_end))
    logging.debug("Source Points: " + str(source_points.compute()))
    logging.debug("Destination Points: " + str(dest_points.compute()))
    #if isCompute:
    distances = cdist(source_points, dest_points) 
    #distances_bool = (distances < cutoff) & (distances > 0)
    #logging.debug(str(distances_bool))
    #xx, yy = np.meshgrid(np.arange(source_start, source_end), np.arange(dest_start, dest_end))
    #logging.debug("xx: " + str(xx))
    #logging.debug("yy: " + str(yy))
    #res=np.array([zip(y,x, z) for x,y,z in zip(xx, yy, distances_bool.T)])
    #true_res = np.array(np.where((distances < cutoff) & (distances > 0)))
    true_res = np.array(np.where(distances < cutoff))
    logging.debug("True Source: %s, Source_start: %d"%(str(true_res[0]), source_start))
    true_res[0] = true_res[0] + source_start # source offset for block
    logging.debug("True Source Adjusted" + str(true_res[0]))
    true_res[1] = true_res[1] + dest_start # dest offset for block
    res=np.array(zip(true_res[0], true_res[1]))
    res=res[res[:,0]<res[:,1], :] # filter duplicate edges (only edges where ind1<ind2)
    #number_pairs = block.shape[0]*block.shape[1]
    #logging.debug("Result Shape: %s Block Shape: %s, Number Pairs: %d"%(str(res.shape), str(block.shape), number_pairs))
    #res=res.reshape(number_pairs, 3)
    logging.debug("Result: " + str(res))
    return res
    #else:
    #    return np.zeros((1,2))
    #return new_block + cdist(source_points, dest_points)<cutoff
    #return np.array(block)
    #return new_block 





    
def benchmark_dask_map_block_1d_sparse_distributed(filename, cutoff=15, direct_output=True ):
    global point_array
    func_name = sys._getframe().f_code.co_name
    results = []
    #cache = Chest(path=os.path.join(BASE_DIRECTORY, "cache"), available_memory=98e9)  

    cluster_details=client.ncores()
    number_nodes = len(cluster_details.keys())
    number_cores = sum(cluster_details.values())  
    global point_array
    with dask.set_options(get=client.get):        
        start = time.time()
        point_array=da.from_npy_stack(filename)
        chunk_size = point_array.chunks[0][0]   
        end_read = time.time()
        results.append("%s,dask-distributed, %s, %d, %d, comet, read_file, %.4f"%(filename, func_name, number_nodes, number_cores, end_read-start))
        chunk_size
        dist_matrix = da.zeros((point_array.shape[0],point_array.shape[0]), chunks=(chunk_size, point_array.shape[0]))
        """map_block_distances operates on point_array """
        da_res=dist_matrix.map_blocks(map_blocks_1d_sparse, chunks=(chunk_size,3), dtype='int')
        res=da_res.compute()
        end_compute = time.time()
        results.append("%s,dask-distributed, %s, %d, %d, comet, compute, %.4f"%(filename, func_name, number_nodes, number_cores, end_compute-end_read))
        results.append("%s,dask-distributed, %s, %d, %d, comet, total, %.4f"%(filename, func_name, number_nodes, number_cores, end_compute-start))    
        print("\n".join(results))



dask_scenarios = [os.path.abspath(os.path.join(OUT_DIR, i)) for i in os.listdir(OUT_DIR)]
shuffle(dask_scenarios)
dask_scenarios


# In[ ]:

for idx, s in enumerate(dask_scenarios):
    #if '839K' in s:
    print "Process: %s (%d/%d)"%(s, idx+1, len(dask_scenarios))
    try:
        benchmark_dask_map_block_1d_sparse_distributed(s)
    except:
        print "Failed! Exception"
    time.sleep(60)



