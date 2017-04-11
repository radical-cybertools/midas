import sys
import time
import numpy as np
from pyspark  import  SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import  KafkaUtils

####### CONFIGURATIONS  #########
METABROKER_LIST = sys.argv[1]
topic = sys.argv[2]
number_of_executors = 2
STREAMING_WINDOW = 60
number_of_partitions = 24
##################################





ssc_start = time.time()
sc = SparkContext(appName="LeafletFinderStreaming")
ssc = StreamingContext(sc, STREAMING_WINDOW)
kafka_dstream = KafkaUtils.createDirectStream(ssc, [TOPIC], {"metadata.broker.list": METABROKER_LIST })
ssc_end = time.time()



