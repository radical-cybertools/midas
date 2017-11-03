from pykafka import KafkaClient
import numpy as np
import os, sys
import time
import datetime
sys.path.append("..")
from pykafka.partitioners import hashing_partitioner

NUMBER_CLUSTER=[100, 100, 100, 100, 100]
NUMBER_POINTS_PER_CLUSTER=[100, 1000, 10000, 100000, 1000000]

#NUMBER_CLUSTER=[100]
#NUMBER_POINTS_PER_CLUSTER=[1000]
NUMBER_DIM=3 # 1 Point == ~62 Bytes
NUMBER_POINTS_PER_MESSAGE=[5000] # 3-D Point == 304 KB
#NUMBER_POINTS_PER_MESSAGE=[10000] # 3-D Point == 304 KB
INTERVAL=120
NUMBER_OF_PRODUCES=1 # 10*60 = 10 minutes
NUMBER_PARTITIONS=384  # set 1 partition per core 
TOPIC_NAME="Throughput"


broker= sys.argv[1]


client = KafkaClient(hosts=broker)
topic = client.topics[TOPIC_NAME]
producer = topic.get_sync_producer(partitioner=hashing_partitioner)

run_timestamp=datetime.datetime.now()
RESULT_FILE= "kafka-throughput-producer-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
STDOUT_FILE = "stdout-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
#STDOUT_LOOP_FILE = "stdout_loop-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"


output_file=open(RESULT_FILE, "w")
output_file.write("Number_Clusters,Number_Points_per_Cluster,Number_Dim,Number_Points_per_Message,Interval,Number_Partitions, Time\n")

stdout_file = open(STDOUT_FILE, 'w')
stdout_file.write('Produce_batch,Num_Messages,Number_APoints/Msg,KB_Transfered,KB/sec\n')

#stdout_loop_file = open(STDOUT_LOOP_FILE,'w')
#stdout_loop_file.write("Number,x_points,Time_to_produce_x_points\n")


def get_random_cluster_points(number_points, number_dim):
    mu = np.random.randn()
    sigma = np.random.randn()
    p = sigma * np.random.randn(number_points, number_dim) + mu
    return p

bytes = 0
num_messages = 0

count = 0
global_start = time.time()
for num_points_per_message in NUMBER_POINTS_PER_MESSAGE:
    for idx, num_cluster in enumerate(NUMBER_CLUSTER):
        count_produces = 0
        num_point_per_cluster = NUMBER_POINTS_PER_CLUSTER[idx]
        while count_produces < NUMBER_OF_PRODUCES:
            start = time.time()
            points = []
            for i in range(num_cluster):
                p = get_random_cluster_points(num_point_per_cluster, NUMBER_DIM)
                points.append(p)
            points_np=np.concatenate(points)
            
            number_batches = points_np.shape[0]/num_points_per_message
            #print "Points Array Shape: %s, Number Batches: %.1f"%(points_np.shape, number_batches)
            last_index=0
            for i in range(number_batches):
                stdout_file.write( "%d,%d,%d,%d,%.1f,%s\n"%\
                                    (last_index,                                                                                           
                                     last_index+num_points_per_message, 
                                     num_messages,
                                     num_points_per_message,
                                     bytes/1024,
                                     bytes/1024/(time.time()-global_start)))
                points_batch = points_np[last_index:last_index+num_points_per_message]
                points_strlist=str(points_batch.tolist())
                producer.produce(points_strlist, partition_key='{}'.format(count))
                count = count + 1
                last_index = last_index + num_points_per_message
                bytes = bytes + len(points_strlist)
                num_messages = num_messages + 1
                #time.sleep(INTERVAL)
            
            end = time.time()
            #stdout_loop_file.write("%d,%d,%.1f\n"%(count_produces,end-start,num_cluster*num_point_per_cluster))
            output_file.write("%d,%d,%d,%d,%d,%d,%.5f\n"%(num_cluster,num_point_per_cluster,NUMBER_DIM, 
                                                       num_points_per_message,INTERVAL,NUMBER_PARTITIONS,(end-start)))
            output_file.flush()
            stdout_file.flush()
            #stdout_loop_file.flush()
            count_produces = count_produces + 1
    
        #time.sleep(INTERVAL)

output_file.close()
