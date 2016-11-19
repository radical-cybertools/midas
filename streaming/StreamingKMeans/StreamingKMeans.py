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
import init_spark_wrangler
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.clustering import StreamingKMeans
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import numpy as np
import msgpack
import msgpack_numpy as m
import urllib, json
import socket
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


def get_number_partitions():
    cmd = "/home/01131/tg804093/work/kafka_2.11-0.10.1.0/bin/kafka-topics.sh --describe --topic %s --zookeeper %s"%(TOPIC, KAFKA_ZK)
    print cmd
    out = check_output(cmd, shell=True)
    number=re.search("(?<=PartitionCount:)[0-9]*", out).group(0)
    return number


NUMBER_PARTITIONS = get_number_partitions()
print "Number Partitions: "   + NUMBER_PARTITIONS

print "Spark Master: " + SPARK_MASTER

run_timestamp=datetime.datetime.now()
RESULT_FILE= "results/spark-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"


try:
    os.makedirs("results")
except:
    pass

output_file=open(RESULT_FILE, "w")

os.environ["SPARK_LOCAL_IP"]=SPARK_LOCAL_IP

pilotcompute_description = {
    "service_url": SPARK_MASTER,
    "spark.driver.host": os.environ["SPARK_LOCAL_IP"]
}


print "SPARK HOME: %s"%os.environ["SPARK_HOME"]
print "PYTHONPATH: %s"%os.environ["PYTHONPATH"]

def get_application_details(sc):
    app_id=sc.applicationId
    url = "http://" + SPARK_LOCAL_IP + ":4040/api/v1/applications/"+ app_id + "/executors"
    max_id = -1
    while True:
        cores = 0
        response = urllib.urlopen(url)
        data = json.loads(response.read())
        print data
        for i in data:
            print "Process %s"%i["id"]
            if i["id"]!="driver":
                cores = cores + i["totalCores"]
                if int(i["id"]) > max_id: max_id = int(i["id"])
        print "Max_id: %d, Number Executors: %d"%(max_id, NUMBER_EXECUTORS)
        if (max_id == (NUMBER_EXECUTORS-1)):
            break
        time.sleep(.1)
            
            
    # http://129.114.58.102:4040/api/v1/applications/
    # http://129.114.58.102:4040/api/v1/applications/app-20160821102227-0003/executors
    return cores


start = time.time()
pilot_spark = PilotSparkComputeService.create_pilot(pilotcompute_description=pilotcompute_description)
sc = pilot_spark.get_spark_context()
spark_cores=get_application_details(sc)
#print str(sc.parallelize([2,3]).collect())
output_file.write("Measurement,Spark Cores,Number Points,Number_Partitions, Time\n")
output_file.write("Spark Startup, %d, %d, %s, %.5f\n"%(spark_cores, -1, NUMBER_PARTITIONS, time.time()-start))
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
        
def pre_process(datetime, rdd):  
    #print (str(type(time)) + " " + str(type(rdd)))    
    start = time.time()    
    points=rdd.map(lambda p: p[1]).flatMap(lambda a: eval(a)).map(lambda a: Vectors.dense(a))
    end_preproc=time.time()
    count = points.count()
    output_file.write("KMeans PreProcess, %d, %d, %s, %.5f\n"%(spark_cores, count, NUMBER_PARTITIONS, end_preproc-start))
    output_file.flush()
    return points
    #points.pprint()
    #model.trainOn(points)

def model_update(rdd):
    count = rdd.count()
    start = time.time()
    lastest_model = model.latestModel()
    lastest_model.update(rdd, decayFactor, timeUnit)    
    end_train = time.time()
    #predictions=model.predictOn(points)
    #end_pred = time.time()    
    output_file.write("KMeans Model Update, %d, %d, %s, %.5f\n"%(spark_cores, count,NUMBER_PARTITIONS, end_train-start))
    output_file.flush()
    #output_file.write("KMeans Prediction, %.3f\n"%(end_pred-end_train))
    #return predictions
    

def model_prediction(rdd):
    pass

    
ssc_start = time.time()    
ssc = StreamingContext(sc, STREAMING_WINDOW)
#kafka_dstream = KafkaUtils.createStream(ssc, KAFKA_ZK, "spark-streaming-consumer", {TOPIC: 1})
#kafka_param: "metadata.broker.list": brokers
kafka_dstream = KafkaUtils.createDirectStream(ssc, [TOPIC], {"metadata.broker.list": METABROKER_LIST })
ssc_end = time.time()    
output_file.write("Spark SSC Startup, %d, %d, %s, %.5f\n"%(spark_cores, -1, NUMBER_PARTITIONS, ssc_end-ssc_start))


#counts=[]
#kafka_dstream.foreachRDD(lambda t, rdd: counts.append(rdd.count()))
#global count_messages 
#count_messages  = sum(counts)
#
#output_file.write(str(counts))
kafka_dstream.count().pprint()

#print str(counts)
#count = kafka_dstream.count().reduce(lambda a, b: a+b).foreachRDD(lambda a: a.count())
#if count==None:
#    count=0
#print "Number of Records: %d"%count


points = kafka_dstream.transform(pre_process)
points.pprint()
points.foreachRDD(model_update)

#predictions=model_update(points)
#predictions.pprint()



#We create a model with random clusters and specify the number of clusters to find
#model = StreamingKMeans(k=10, decayFactor=1.0).setRandomCenters(3, 1.0, 0)
#points=kafka_dstream.map(lambda p: p[1]).flatMap(lambda a: eval(a)).map(lambda a: Vectors.dense(a))
#points.pprint()


# Now register the streams for training and testing and start the job,
# printing the predicted cluster assignments on new data points as they arrive.
#model.trainOn(trainingStream)
#result = model.predictOnValues(testingStream.map(lambda point: ))
#result.pprint()

# Word Count
#lines = kvs.map(lambda x: x[1])
#counts = lines.flatMap(lambda line: line.split(" ")) \
#        .map(lambda word: (word, 1)) \
#        .reduceByKey(lambda a, b: a+b)
#counts.pprint()

ssc.start()
ssc.awaitTermination()
ssc.stop(stopSparkContext=True, stopGraceFully=True)

output_file.close()
