from __future__ import division
import numpy as np
import subprocess
import os, sys, time

SPARK_HOME="/work/03170/tg824689/wrangler/SparkThroughput/spark-2.1.1-bin-hadoop2.7/"
os.environ["SPARK_HOME"]=SPARK_HOME
print "Init Spark: " + SPARK_HOME

os.environ["PYSPARK_PYTHON"]="/home/03170/tg824689/miniconda2/bin/python"
os.environ["PYTHONPATH"]= os.path.join(SPARK_HOME, "python")+":" + os.path.join(SPARK_HOME, "python/lib/py4j-0.10.1-src.zip")

sys.path.insert(0, os.path.join(SPARK_HOME, "python"))
#sys.path.insert(0, os.path.join(SPARK_HOME, 'python/lib/py4j-0.9-src.zip'))
sys.path.insert(0, os.path.join(SPARK_HOME, 'python/lib/py4j-0.10.4-src.zip'))
sys.path.insert(0, os.path.join(SPARK_HOME, 'bin') )

# import Spark Libraries
from pyspark import SparkContext, SparkConf, Accumulator, AccumulatorParam
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.mllib.linalg import Vector

conf = SparkConf().setAppName("SparkTest").setMaster("spark://%s:7077"%sys.argv[1])
sc = SparkContext(conf=conf)
tasks = [1,2,4,8,16,32,64,128,256,512,1024,2048,4096,8192,16384,32768,65536,131072]

for i in range(0,10):
    for taskNum in tasks:
        rdd = sc.parallelize(range(taskNum))
        start_time = time.time()
        rdd.map(lambda a: subprocess.check_output(["/bin/sleep",'0'])).saveAsTextFile("/gpfs/flash/users/tg824689/spark4-out-%d-%d"%(i,taskNum))
        end_time= time.time()
        print("Iteation %d Spark-2.1.1, %d, Runtime, %.4f, Througthput %.4f"%(i+1, taskNum, (end_time-start_time),taskNum/(end_time-start_time)))
