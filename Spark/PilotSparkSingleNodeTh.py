import sys
from pyspark import SparkContext
from datetime import datetime
import time
import numpy as np
import subprocess


if __name__=="__main__":

    tasks = [1,2,4,8,16,32,64,128,256,512,1024,2048,4096,8192,16384,32768,65536,131072]
    for i in range(0,10):
        timings=np.zeros((18,2))
        for taskNum in tasks:
            sc = SparkContext(appName="ThroughputTest")
            rdd = sc.parallelize(range(taskNum))
            start_time = time.time()
            rdd.map(lambda a: subprocess.check_output(["/bin/sleep",'0'])).saveAsTextFile("/gpfs/flash/users/tg824689/spark4-out-%d-%d"%(i,taskNum))
            end_time= time.time()
            sc.stop()
            print("Iteation %d Spark-2.1.1, %d, Runtime, %.4f"%(i+1, taskNum, (end_time-start_time)))
            timings[tasks.index(taskNum),0]=start_time
            timings[tasks.index(taskNum),1]=end_time
        np.save('timings2_%d.npz.npy'%i,timings)
