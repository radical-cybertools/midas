import os
import sys
import time
import datetime
start = time.time()
import logging
logging.basicConfig(level=logging.WARN)
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark  import  SparkContext, SparkConf
import numpy as np
import urllib2


#######################################################################################
# CONFIGURATIONS
METABROKER_LIST= sys.argv[1]
TOPIC= sys.argv[2]
NUMBER_PARTITIONS = int(sys.argv[3])
STREAMING_WINDOW=60
#######################################################################################

run_timestamp=datetime.datetime.now()
RESULT_FILE= "results/kafka-streaming-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"

try:
    os.makedirs("results")
except:
    pass

output_file=open(RESULT_FILE, "w")

start = time.time()

#output_file.write("Measurement,Number_Partitions, Time\n")
#output_file.write("Spark Startup, %s, %.5f\n"%(NUMBER_PARTITIONS, time.time()-start))
#output_file.flush()
#######################################################################################

decayFactor=1.0
timeUnit="batches"
model = StreamingKMeans(k=10, decayFactor=decayFactor, timeUnit=timeUnit).setRandomCenters(3, 1.0, 0)

#def printOffsetRanges(rdd):
#    for o in offsetRanges:
#        print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

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
    #output_file.write("KMeans PreProcess, %d, %s, %.5f\n"%(count, NUMBER_PARTITIONS, end_preproc-start))
    #output_file.flush()
    return points

## OK
def model_update(rdd):
    count = rdd.count()
    start = time.time()
    lastest_model = model.latestModel()
    lastest_model.update(rdd, decayFactor, timeUnit)
    end_train = time.time()
    #output_file.write("KMeans Model Update, %d, %s, %.5f\n"%(count,NUMBER_PARTITIONS, end_train-start))
    output_file.flush()

appName="PythonSparkStreamingKafkaKMeans"
conf = SparkConf().setAppName(appName).set('spark.metrics.conf.*.sink.csv.class','org.apache.spark.metrics.sink.CsvSink').set('spark.metrics.conf.*.sink.csv.directory','./')
sc = SparkContext(conf=conf)

ssc_start = time.time()
ssc = StreamingContext(sc, STREAMING_WINDOW)
kafka_dstream = KafkaUtils.createDirectStream(ssc, [TOPIC], {"metadata.broker.list": METABROKER_LIST })
ssc_end = time.time()
#output_file.write("Spark SSC Startup, %d, %s\n"%( NUMBER_PARTITIONS, str(ssc_end-ssc_start)))


kafka_dstream.count().pprint()

points = kafka_dstream.transform(pre_process)
points.pprint()
points.foreachRDD(model_update)


try:
    ssc.start()
    ssc.awaitTermination()
    ssc.stop(stopSparkContext=True, stopGraceFully=True)

    output_file.close()

    print 'Time is: %d \n' % time.time() - start
finally:
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
    sys.stdout.flush()
