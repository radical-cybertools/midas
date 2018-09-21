import dask.bag as db
import numpy as np
from time import time
from dask.distributed import Client
from distributed.diagnostics.plugin import SchedulerPlugin
import numpy as np
import argparse
import networkx as nx
from pprint import pprint
from sklearn.neighbors import BallTree

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

def find_parcc(data,cutoff=15.0):
    print 'Starting Running'
    window,index = data[0]
    num = window[0].shape[0]
    i_index = index[0]
    j_index = index[1]
    graph = nx.Graph()
    if i_index == j_index:
       train = window[0]
       test = window[1]
    else:
        train = np.vstack([window[0],window[1]])
        test  = np.vstack([window[0],window[1]])
    tree = BallTree(train, leaf_size=40)
    edges = tree.query_radius(test, cutoff)
    print 'Query Done'
    edge_list=[list(zip(np.repeat(idx, len(dest_list)), \
            dest_list)) for idx, dest_list in enumerate(edges)]

    edge_list_flat = np.array([list(item) \
            for sublist in edge_list for item in sublist])
    if i_index == j_index:
        res = edge_list_flat.transpose()
        res[0] = res[0] + i_index - 1
        res[1] = res[1] + j_index - 1
    else:
        removed_elements = list()
        for i in range(edge_list_flat.shape[0]):
            if (edge_list_flat[i,0]>=0 and edge_list_flat[i,0]<=num-1) and (edge_list_flat[i,1]>=0 and edge_list_flat[i,1]<=num-1) or\
               (edge_list_flat[i,0]>=num and edge_list_flat[i,0]<=2*num-1) and (edge_list_flat[i,1]>=num and edge_list_flat[i,1]<=2*num-1):
                removed_elements.append(i)
        res = np.delete(edge_list_flat,removed_elements,axis=0).transpose()
        res[0] = res[0] + i_index - 1
        res[1] = res[1] -num + j_index - 1

    edges=[(res[0,k],res[1,k]) for k in range(0,res.shape[1])]
    graph.add_edges_from(edges)

    # partial connected components

    subgraphs = nx.connected_components(graph)
    comp = [g for g in subgraphs]
    graph_time=time()
    return comp


if __name__=="__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="File with the atoms positions")
    parser.add_argument("window", help="i coordinate at the distance matrix")
    parser.add_argument("cutoff", help="The cutoff value for the edge existence")
    parser.add_argument("scheduler",help="The scheduler address. Should be IP:PORT (Usually PORT=8786)")
    parser.add_argument("--profile",help="Profile filename",default='./Profile.txt')
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
        for j in range(1,matrix_size+1,part_size):
            arraged_coord.append(([atoms[i-1:i-1+part_size],atoms[j-1:j-1+part_size]],[i,j]))

    overhead = time()
    print "Ready",len(arraged_coord)
    parAtoms = db.from_sequence(arraged_coord,npartitions=len(arraged_coord))
    print 'Distributed'
    parAtomsMap = parAtoms.map_partitions(find_parcc)
    print 'Map done'
    Components = parAtomsMap.compute(get=client.get)

    edge_list_time = time()
    result = list(Components)
    while len(result)!=0:
        item1 = result[0]
        result.pop(0)
        ind = []
        for i, item2 in enumerate(Components):
            if item1.intersection(item2):
                item1=item1.union(item2)
                ind.append(i)
        ind.reverse()
        [Components.pop(j) for j in ind]
        Components.append(item1)

    connComp = time()
    indices = [np.sort(list(g)) for g in Components]
    np.save('components.npz.npy',indices)
    end = time()

    client.run_on_scheduler(removeCustomProfiler)
    incoming = client.run(lambda dask_worker: dask_worker.outgoing_transfer_log)
    outgoing = client.run(lambda dask_worker: dask_worker.incoming_transfer_log)
    with open(args.profile+'.in','w') as incomfp:
        pprint(incoming,stream=incomfp)
    with open(args.profile+'.outg','w') as outfp:
        pprint(outgoing,stream=outfp)
    client.shutdown()

    print 'Start,Overhead,Init,AdjMatrix,ConnComp,End\n'
    print '%f,%f,%f,%f,%f'%(start_time,overhead,edge_list_time,connComp,end)
