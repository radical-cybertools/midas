import dask.array as da
from dask.diagnostics import Profiler, ResourceProfiler, CacheProfiler
from dask import delayed
import dask
from dask import multiprocessing
from dask.multiprocessing import get
import distributed
import numpy as np
from time import sleep


@delayed
def inc(x):
    sleep(60)
    return x + 1

out = list()
for i in range(0,16):
    out.append(inc(i))
    
total = delayed(np.array)(out)

#total.visualize(filename='ThreadedDaskGraph',format='pdf')
for i in range(0,10):
    print "Run Number %d"%(i+1)
    with Profiler() as prof, ResourceProfiler() as rprof, CacheProfiler() as cprof:
        total.compute(get=dask.threaded.get)

    dask.diagnostics.profile_visualize.visualize(prof,'threaded_prof%2d.html'%i,
                                            show=False,save=True)
    file = open('threaded_prof%2d'%i,'w')
    for task in prof.results:
        file.write(task.__str__())
        file.write('\n')

    file.close()
