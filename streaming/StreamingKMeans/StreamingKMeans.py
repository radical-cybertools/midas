#!/bin/python
#
# /home/01131/tg804093/work/spark-2.0.2-bin-hadoop2.6/bin/spark-submit --master spark://c251-102.wrangler.tacc.utexas.edu:7077 --packages  org.apache.spark:spark-streaming-kafka-0-10_2.11:2.0.2 --files saga_hadoop_utils.py StreamingKMeans.py

import os
import sys
import pickle
import time
import datetime
start = time.time()
import logging
logging.basicConfig(level=logging.WARN)
sys.path.append("../util")
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark  import  SparkContext
import numpy as np
import msgpack
import msgpack_numpy as m
import urllib, json
import socket
import saga_hadoop_utils
import re
from subprocess import check_output

#######################################################################################
# CONFIGURATIONS
# Get current cluster setup from work directory                                  
METABROKER_LIST=",".join(kafka_details[0])
TOPIC='KmeansList'
NUMBER_EXECUTORS=1
STREAMING_WINDOW=60
#######################################################################################


NUMBER_PARTITIONS = 2

print "Number Partitions: "   + NUMBER_PARTITIONS   #TODO: Fix this




start = time.time()

output_file.write("Measurement,Spark Cores,Number Points,Number_Partitions, Time\n")
output_file.write("Spark Startup, %d, %s, %.5f\n"%(NUMBER_PARTITIONS, time.time()-start))
output_file.flush()
#######################################################################################

decayFactor=1.0
timeUnit="batches"
model = StreamingKMeans(k=10, decayFactor=decayFactor, timeUnit=timeUnit).setRandomCenters(3, 1.0, 0)

def printOffsetRanges(rdd):
    for o in offsetRanges:
        print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

def count_records(rdd):    
    print str(type(rdd))
    if rdd!=None:
        return rdd.collect()
    
    return [0]

## OK        
def pre_process(datetime, rdd):  
    start = time.time()    
    points=rdd.map(lambda p: p[1]).flatMap(lambda a: eval(a)).map(lambda a: Vectors.dense(a))
    end_preproc=time.time()
    count = points.count()
    output_file.write("KMeans PreProcess, %d, %s, %.5f\n"%(count, NUMBER_PARTITIONS, end_preproc-start))
    output_file.flush()
    return points

## OK
def model_update(rdd):
    count = rdd.count()
    start = time.time()
    lastest_model = model.latestModel()
    lastest_model.update(rdd, decayFactor, timeUnit)    
    end_train = time.time()
    output_file.write("KMeans Model Update, %d, %s, %.5f\n"%(count,NUMBER_PARTITIONS, end_train-start))
    output_file.flush()
    


    
ssc_start = time.time()    
ssc = StreamingContext(sc, STREAMING_WINDOW)
kafka_dstream = KafkaUtils.createDirectStream(ssc, [TOPIC], {"metadata.broker.list": METABROKER_LIST })
ssc_end = time.time()    
output_file.write("Spark SSC Startup, %d, %s, %.5f\n"%( NUMBER_PARTITIONS, ssc_end-ssc_start))



kafka_dstream.count().pprint()




points = kafka_dstream.transform(pre_process)
points.pprint()
points.foreachRDD(model_update)



ssc.start()
ssc.awaitTermination()
ssc.stop(stopSparkContext=True, stopGraceFully=True)

output_file.close()
