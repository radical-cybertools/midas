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
from pyspark.streaming.listener import StreamingListener

import numpy as np
import urllib2


#######################################################################################
#spark metrics perfomance
class BatchInfoCollector(StreamingListener):

    def __init__(self):
        super(StreamingListener, self).__init__()
        self.batchInfosCompleted = []
        self.batchInfosStarted = []
        self.batchInfosSubmitted = []

    def onBatchSubmitted(self, batchSubmitted):
        self.batchInfosSubmitted.append(batchSubmitted.batchInfo())

    def onBatchStarted(self, batchStarted):
        self.batchInfosStarted.append(batchStarted.batchInfo())


    def onBatchCompleted(self, batchCompleted):
        info = batchCompleted.batchInfo()
        submissionTime = datetime.datetime.fromtimestamp(info.submissionTime()/1000).isoformat()
        
        output_spark_metrics.write("%s, %s, %d, %d, %d, %d,%s\n"%(str(info.batchTime()), submissionTime, \
                                                         info.schedulingDelay(), info.totalDelay(),\
                                                         info.processingDelay() ,info.numRecords(), SCENARIO))
        output_spark_metrics.flush()
        self.batchInfosCompleted.append(batchCompleted.batchInfo())

#######################################################################################

#######################################################################################
# CONFIGURATIONS
METABROKER_LIST= sys.argv[1]
TOPIC= sys.argv[2]
NUMBER_PARTITIONS = int(sys.argv[3])
STREAMING_WINDOW=60
SCENARIO="1_Producer_Count"
#######################################################################################

run_timestamp=datetime.datetime.now()
RESULT_FILE= "results/kafka-streaming-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
SPARK_RESULT_FILE="results/spark-metrics-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"


try:
    os.makedirs("results")
except:
    pass

output_file=open(RESULT_FILE, "w")

output_spark_metrics=open(SPARK_RESULT_FILE, "w")
output_spark_metrics.write("BatchTime, SubmissionTime, SchedulingDelay, TotalDelay, NumberRecords, Scenario\n")
start = time.time()

#output_file.write("Measurement,Number_Partitions, Time\n")
#output_file.write("Spark Startup, %s, %.5f\n"%(NUMBER_PARTITIONS, time.time()-start))
#output_file.flush()
#######################################################################################

decayFactor=1.0
timeUnit="batches"
model = StreamingKMeans(k=10,decayFactor=decayFactor, timeUnit=timeUnit).setRandomCenters(3, 1.0, 0)

#def printOffsetRanges(rdd):
#    for o in offsetRanges:
#        print "%s %s %s %s" % (o.topic, o.partition, o.fromOffset, o.untilOffset)

def count_records(rdd):
    #print str(type(rdd))
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
#conf = SparkConf().setAppName(appName).set('spark.metrics.conf.*.sink.csv.class','org.apache.spark.metrics.sink.CsvSink').set('spark.metrics.conf.*.sink.csv.directory','./')
#sc = SparkContext(conf=conf)
sc = SparkContext()

ssc_start = time.time()
ssc = StreamingContext(sc, STREAMING_WINDOW)


batch_collector = BatchInfoCollector()
ssc.addStreamingListener(batch_collector)


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

finally:
    ssc.stop(stopSparkContext=True, stopGraceFully=True)
    output_file.flush()
