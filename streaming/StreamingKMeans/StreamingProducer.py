from pykafka import KafkaClient
import numpy as np
import os
import time
import datetime
from pykafka.partitioners import hashing_partitioner

NUMBER_CLUSTER=[100, 100, 100, 100, 100]
NUMBER_POINTS_PER_CLUSTER=[100, 1000, 10000, 100000, 1000000]
NUMBER_DIM=3 
NUMBER_POINTS_PER_MESSAGE=5000
INTERVAL=60
NUMBER_OF_PRODUCES=10 # 10*60 = 10 minutes
NUMBER_PARTITIONS=96
TOPIC_NAME="KmeansList"


zkKafka=saga_hadoop_utils.get_kafka_config_details(os.path.expanduser('~'))[1]
client = KafkaClient(zookeeper_hosts=zkKafka)
cmd="%s/bin/kafka-topics.sh --delete --zookeeper %s --topic %s"%(KAFKA_HOME, zkKafka, TOPIC_NAME)
print cmd
os.system(cmd)

cmd="%s/bin/kafka-topics.sh --create --zookeeper %s --replication-factor 1 --partitions %d --topic %s"%(KAFKA_HOME, zkKafka, NUMBER_PARTITIONS, TOPIC_NAME)
print cmd
os.system(cmd)

cmd="%s/bin/kafka-topics.sh --describe --zookeeper %s --topic %s"%(KAFKA_HOME, zkKafka, TOPIC_NAME)
print cmd
os.system(cmd)


#client = KafkaClient(hosts='c251-142.wrangler.tacc.utexas.edu:9092')
topic = client.topics[TOPIC_NAME]
producer = topic.get_sync_producer(partitioner=hashing_partitioner)
#consumer = topic.get_simple_consumer()

run_timestamp=datetime.datetime.now()
RESULT_FILE= "results/kafka-producer-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"

try:
    os.makedirs("results")
except:
    pass

output_file=open(RESULT_FILE, "w")
output_file.write("Number_Clusters,Number_Points_per_Cluster,Number_Dim,Number_Points_per_Message,Interval,Number_Partitions, Time\n")

def get_random_cluster_points(number_points, number_dim):
    mu = np.random.randn()
    sigma = np.random.randn()
    p = sigma * np.random.randn(number_points, number_dim) + mu
    return p



for idx, num_cluster in enumerate(NUMBER_CLUSTER):
    count_produces = 0
    num_point_per_cluster = NUMBER_POINTS_PER_CLUSTER[idx]
    
    count = 0
    while count_produces < NUMBER_OF_PRODUCES:
        start = time.time()
        points = []
        for i in range(num_cluster):    
            p = get_random_cluster_points(num_point_per_cluster, NUMBER_DIM)
            points.append(p)
        points_np=np.concatenate(points)
        
        number_batches = points_np.shape[0]/NUMBER_POINTS_PER_MESSAGE
        print "Points Array Shape: %s, Number Batches: %.1f"%(points_np.shape, number_batches)
        last_index=0
        for i in range(number_batches):
            print "Produce Batch: %d - %d"%(last_index, last_index+NUMBER_POINTS_PER_MESSAGE)
            points_batch = points_np[last_index:last_index+NUMBER_POINTS_PER_MESSAGE]
            points_strlist=str(points_batch.tolist())
            producer.produce(points_strlist, partition_key='{}'.format(count))
            count = count + 1
            last_index = last_index + NUMBER_POINTS_PER_MESSAGE
        
        end = time.time()
        print "Number: %d, Time to produce %d points: %.1f"%(count_produces, num_cluster*num_point_per_cluster, end-start)
        output_file.write("%d,%d,%d,%d,%d,%d,%.5f\n"%(num_cluster,num_point_per_cluster,NUMBER_DIM, 
                                                   NUMBER_POINTS_PER_MESSAGE,INTERVAL,NUMBER_PARTITIONS,(end-start)))
        output_file.flush()
        count_produces = count_produces + 1
        time.sleep(INTERVAL)

    time.sleep(INTERVAL)

output_file.close()

