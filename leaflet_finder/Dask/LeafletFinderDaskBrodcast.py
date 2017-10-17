import dask.bag as db
from dask import delayed
from time import time
from dask.distributed import Client
import sys
from distributed.diagnostics.plugin import SchedulerPlugin
from scipy.spatial.distance import cdist
import numpy as np
import networkx as nx
import argparse

def submitCustomProfiler(profname,dask_scheduler):
    prof = MyProfiler(profname)
    dask_scheduler.add_plugin(prof)

def removeCustomProfiler(dask_scheduler):
    dask_scheduler.remove_plugin(dask_scheduler.plugins[-1])

class MyProfiler(SchedulerPlugin):
    def __init__(self,profname):
        self.profile = profname

    def transition(self,key,start,finish,*args,**kargs):
        if start == 'processing' and finish == 'memory':
            with open(self.profile,'a') as ProFile:
                ProFile.write('{}\n'.format([key,start,finish,kargs['worker'],kargs['thread'],kargs['startstops']]))

def find_edges(data,cutoff=15.0):
    map_start=time()
    window,index = data[0]
    par_list = (cdist(window[0],atoms)<cutoff)
    adj_list=np.where(par_list==True)
    adj_list = np.vstack(adj_list)
    adj_list[0] = adj_list[0]+index-1
    if adj_list.shape[1]==0:
        adj_list=np.zeros((2,1))
    map_return=time()

    return [adj_list,index,map_start,map_return]

if __name__=="__main__":
    
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="File with the atoms positions")
    parser.add_argument("window", help="i coordinate at the distance matrix")
    parser.add_argument("cutoff", help="The cutoff value for the edge existence")
    parser.add_argument("scheduler",help="The scheduler address. Should be IP:PORT (Usually PORT=8786")
    parser.add_argument("profile",help="Profile filename",default='./Profile.txt')
    args = parser.parse_args()
    cutoff = int(args.cutoff)
    part_size = int(args.window)
    

    client = Client(args.scheduler)
    client.run_on_scheduler(submitCustomProfiler,args.profile)

    start_time = time()
    atoms = np.load(args.filename)
    matrix_size = atoms.shape[0]
    arraged_coord = list()
    for i in range(1,matrix_size+1,part_size):
        arraged_coord.append((atoms[i-1:i-1+part_size],i))
    
    overhead = time()
    client.scatter(atoms)
    parAtoms = db.from_sequence(arraged_coord,npartitions=len(arraged_coord))

    parAtomsMap = parAtoms.map_partitions(find_edges,cutoff=cutoff)

    EdgeList = parAtomsMap.compute()
    
    edge_list_time = time()

    adj_list=list()
    timings=list()
    for i in range(0,len(EdgeList),4):
        timings.append([EdgeList[i+1],EdgeList[i+2],EdgeList[i+3]])
        adj_list.append(EdgeList[i])
    adj_list=np.hstack(adj_list)
    edges=[(adj_list[0,k],adj_list[1,k]) for k in range(0,adj_list.shape[1])]

    time_to_create_adj_matrix = time()
    #np.save('test_adj_matrix.npz.npy',adj_matrix)
    ##
    graph = nx.Graph(edges)
    subgraphs = nx.connected_components(graph)
    indices = [np.sort(list(g)) for g in subgraphs]
    connComp = time()
    np.save('components.npz.npy',indices)
    end = time()

    client.run_on_scheduler(removeCustomProfiler)
    client.shutdown()

    print 'Start,Overhead,AdjMatrix,ConnComp,End\n'
    print '%f,%f,%f,%f,%f'%(start_time,overhead,time_to_create_adj_matrix,connComp,end)

