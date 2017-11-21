import sys
import ast
import time
import pickle
import multiprocessing as mp
from pykafka import KafkaClient
from scipy.spatial  import distance
import numpy as np
import redis


#### consumer messages from kafka

def get_data_from_kafka(kafka_messages, window, output_queue):


    start_consumption = time.time()
    Nmessages = 0
    while time.time() - start_consumption < window:
        message = consumer.consume(block=True)
        data_np = np.array(ast.literal_eval(message.value))
        output_queue.put(data_np)   # make sure this is correct
        Nmessages+=1  # save that
        
    return

### return a np.array of the elments

def elements_of_consumed_batch(input_queue):

    while input_queue.empty():
        pass

    cur_data_batch = input_queue.get()

    return cur_data_batch



# mapper :
# input : np array of elements:

def get_clusters():

    serialized_clusters =  r.get('means')

    return pickle.loads(serialized_clusters)


def save_sums_to_redis():

    return


def calculate_distances(elements,centroids):      # np.array of 3-d data

   
    return distance.cdist(elements, centroids, 'euclidean') # row: elements , column :centroids 

    
def find_partial_sums(dist, centroids,elements):
    """
    Calculates the partial sums of each cluster

    Parameters
    ----------


    Returns
    -------

    """
    ncentroids = centroids.shape[0]
    dtype = str(centroids.shape) + 'float64,' + str(centroids.shape[0]) + ',1)float32'
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

    zkKafka =  sys.argv[1]
    redis_host = redis_host=  sys.argv[2]

    client = KafkaClient(zookeeper_hosts=zkKafka)
    topic = client.topics['Throughput']
    consumer = topic.get_simple_consumer(reset_offset_on_start=True)
    r = redis.StrictRedis(host=redis_host, port=6379, db=0)

    ## settings ##
    window = 60
    #--------------#

    # multiprocessing settings
    data_batches = mp.Queue()
    processes = [mp.Process(target=get_data_from_kafka, args=(data_batches,)), mp.Process(target=elements_of_consumed_batch, args=(data_batches,))]   #TODO: fix this
    # Run processes
    for p in processes:
        p.start()

    centroids = get_clusters()
    dist = calculate_distances(elements,centroids)
    partial_sums = find_partial_sums(dist,centroids)
    save_sums_to_redis()
