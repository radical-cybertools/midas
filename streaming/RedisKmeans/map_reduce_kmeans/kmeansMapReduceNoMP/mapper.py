import sys, ast, time, pickle, redis
import numpy as np
from pykafka import KafkaClient
from pykafka.partitioners import hashing_partitioner
from scipy.spatial import distance
import time


## consuming function
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

# processing functions

def get_clusters():

    serialized_clusters =  r.get('means')
    return pickle.loads(serialized_clusters)


def save_sums_to_redis(partial_sums):
    r.rpush('partial_sums',pickle.dumps(partial_sums))
    return

def calculate_distances(elements,centroids):      # np.array of 3-d data

    return distance.cdist(elements, centroids, 'euclidean') # row: elements , column :centroids 


def find_partial_sums(dist, centroids,elements):

    #ncentroids = centroids.shape[0]
    dtype = str(centroids.shape) + 'float64,(' + str(centroids.shape[0]) + ',1)float32'
    sum_centroids =  np.zeros(1, dtype=dtype)    # first column is the sum of centroids 2nd is the number of elements
                                                 #access sum of centroids [0][0]
                                                 # acess sum of elements: [0][1]
    centroid_pos = np.argmin(dist, axis=1)  #  index: element id - value:  closest centroid_id

    ## sum all distances of each cluster 
    for i in  xrange(len(elements)):
        centroid = centroid_pos[i]
        sum_centroids[0][0][centroid] += elements[i]  # add also number of elements
        sum_centroids[0][1][centroid] +=1  # added one element to that cluster

    return  sum_centroids







if __name__ == "__main__":

    ## settings
    #zkKafka = 'localhost:2181'
    broker =  sys.argv[1]   #'localhost:9092'
    redis_host = sys.argv[2]  #'localhost'
    ids =  int(sys.argv[3])
    partitions = int(sys.argv[4])
    #kafka_messages = 10000
    window = 60000  # in miliseconds

    f = open('mapper_data_%d.csv' % ids,'w')



    client = KafkaClient(hosts=broker)
    topic = client.topics['Throughput']
    consumer = topic.get_simple_consumer(reset_offset_on_start=True,consumer_timeout_ms=window)
    r = redis.StrictRedis(host=redis_host, port=6379, db=0)

    start = time.time() 
    ## consume data and save them to an array
    data_batch = get_data_from_kafka(window)
    consume_end = time.time()
    astring = 'Consume, %d , %d \n' % (consume_end - start, partitions)
    f.write(astring)
    
    ## assign new points to clusters
    start = time.time()
    centroids = get_clusters()
    elements = np.concatenate(data_batch,axis=0) # fix shape
    elements = np.concatenate(data_batch,axis=0).reshape(elements.size/3,3)[0]
    dist = calculate_distances(data_batch[0], centroids)
    partial_sums = find_partial_sums(dist, centroids, elements)
    save_sums_to_redis(partial_sums)
    end_process = time.time()

    astring = 'Assign, %d , %d\n ' % (end_process - start, partitions)
    f.write(astring)

    f.close()








