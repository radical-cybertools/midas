import dask.array as da
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler
from dask import delayed
import dask
from dask import multiprocessing
from dask.multiprocessing import get
import distributed
import numpy as np
from time import sleep
from time import time
from dask.distributed import Client
import sys

@delayed
def inc(x):
    start = time()
    sleep(60)
    stop = time()
    return (start,stop)

for j in range(0,10):
    c = Client(sys.argv[1])
    
    out = list()
    for i in range(0,64):
        out.append(inc(i))
        
    total = delayed(np.array)(out)
    overall_start = time()
    result = total.compute(get=c.get)
    overall_stop = time()
    c.shutdown()

    timings=np.array([[overall_start,overall_stop],result])
    np.save('timings_48_%02d.npz.npy'%(j+1),timings)
