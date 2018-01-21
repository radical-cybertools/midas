import sys, ast, time, pickle, redis
import numpy as np
from pykafka import KafkaClient
from pykafka.partitioners import hashing_partitioner
from scipy.spatial import distance



def get_data_from_kafka(window):

    start_consumption = time.time()
    Nmessages = 0
    data = []
    while time.time() - start_consumption < window:
        message = consumer.consume(block=True) 
        if message!=None:
            message = message.value
            data_np = np.array(ast.literal_eval(message))
            data.append(data_np)
            Nmessages+=1  # save that
        else:
            break

    return data





if __name__ == "__main__":

    ## settings
    #zkKafka = 'localhost:2181'
    broker =  sys.argv[1]   #'localhost:9092'
    redis_host = sys.argv[2]  #'localhost'
    #kafka_messages = 10000
    window = 25000  # in miliseconds



    client = KafkaClient(hosts=broker)
    topic = client.topics['Throughput']
    consumer = topic.get_simple_consumer(reset_offset_on_start=True,consumer_timeout_ms=window)
    r = redis.StrictRedis(host=redis_host, port=6379, db=0)


    ## consume data and save them to an array
    data_batch = get_data_from_kafka(window)


    ## assign new points to clusters
    centroids = get_clusters()
    elements = np.concatenate(data_batch,axis=0) # fix shape
    elements = np.concatenate(data_batch,axis=0).reshape(elements.size/3,3)[0]
    dist = calculate_distances(data_batch[0], centroids)
    partial_sums = find_partial_sums(dist, centroids, elements)
    save_sums_to_redis(partial_sums)






