from pykafka import KafkaClient
import sys, os
import numpy as np
import time
import datetime
import dateutil.parser
import ast
import sklearn.cluster
import pickle
import redis


zkKafka='c251-122.wrangler.tacc.utexas.edu:2181'
redis_host='c251-123'


client = KafkaClient(zookeeper_hosts=zkKafka)
topic = client.topics['Throughput']
consumer = topic.get_simple_consumer(reset_offset_on_start=True)
r = redis.StrictRedis(host=redis_host, port=6379, db=0)

def put_model(model):
    r.set('kmeans', pickle.dumps(model))
    
def get_model():
    return pickle.loads(r.get("kmeans"))



def process_messages_kmeans_redis(number_messages=1, cu_id=0, total_number_cus=1):
    #global result
    #global number_threads 
    #global number_points_per_message
    
    print "CU: %d, Process %d messages"%(cu_id, number_messages)
    count = 0
    while count < number_messages:
        start = time.time()
        message = consumer.consume(block=True)
        end_kafka = time.time()
        data_np = np.array(ast.literal_eval(message.value))
        num_points = data_np.shape[0]
        number_dimensions = data_np.shape[1]
        number_points_per_message = num_points
        end_parsing = time.time()
        kmeans = get_model()
        number_centroids = kmeans.cluster_centers_.shape[0]
        end_model_get = time.time()
        kmeans = kmeans.partial_fit(data_np)
        end_kmeans = time.time()
        put_model(kmeans)
        end_model_put = time.time()    
        res =  "kmeans-kafka,   %d, %d, %d, %d, %.5f\n"%(num_points, number_dimensions, number_centroids, total_number_cus, end_kafka-start) 
        res += "kmeans-parsing, %d, %d, %d, %d, %.5f\n"%(num_points, number_dimensions, number_centroids, total_number_cus, end_parsing-end_kafka) 
        res += "kmeans-model-get,   %d, %d, %d, %d, %.5f\n"%(num_points, number_dimensions, number_centroids, total_number_cus, end_model_get-end_parsing) 
        res += "kmeans-model-redis,   %d, %d, %d, %d, %.5f\n"%(num_points, number_dimensions, number_centroids, total_number_cus, end_kmeans-end_model_get) 
        res += "kmeans-model-put,   %d, %d, %d, %d, %.5f\n"%(num_points, number_dimensions, number_centroids, total_number_cus, end_model_put-end_kmeans)
        if count % 100 == 0:
            print "Messages processed: %d"%count
        count += 1
    
    return res

    
if __name__ == "__main__":
    if len(sys.argv)!=6:
        print """Usage:
            python %s <number_messages> <cu_id> <total_number_cus> <zkKafka> <redis>
              """%sys.argv[0]
        sys.exit(-1)

    number_of_messages = int(sys.argv[1])
    cu_id = int(sys.argv[2])
    total_number_cus = int(sys.argv[3])
    print "CU: %d, Process %d messages from Kafka: %s"%(cu_id, number_of_messages, zkKafka)
    res = process_messages_kmeans_redis(number_of_messages, cu_id, total_number_cus)
    print res
    
    
    
    
    
    
