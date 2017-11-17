from scipy.spatial  import distance
import multiprocessing as mp
from pykafka import KafkaClient
import ast
import time
import numpy as np
import redis

## settings ##
window = 60
#--------------#


#### consumer messages from kaka

def get_data_from_kafka(kafka_messages,window, output_queue):


    start_consumtion = time.time()
    while time.time() - start_consumption<window:
        message = consumer.consume(block=True)
        data_np = np.array(ast.literal_eval(message.value))
        output.put(data_np)   # make sure this is correct
        
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

    return


def save_sums_to_redis():

    return


def calculate_distances(elements,centroids):      # np.array of 3-d data

   
    return distance.cdist(elements, centroids, 'euclidean') # row: elements , column :centroids 

    
def find_partial_sums(dist, centroids):

    sum_centroids = np.zeros(((centroids.shape[0],2))    # first column is the sum of centroids 2nd is the number of elements
    min_values = np.amin(dist, axis=1)     #  index: element_id  - value:  distance from closest centroid
    centroid_pos = np.argmin(distances,axis=1)  #  index: element id - value:  closest centroid_id

    ## sum all distances of each cluster 
    for i in  xrange(len(centroid_pos)):
        centroid = centroid_pos[i]
        sum_centroids[centroid][0] = += min_values[i]  # add also number of elements
        sum_centrods[centroid][1] +=1  # added one element to that cluster

    
    return

if __name__ == "__main__":

    zkKafka=  sys.argv[1]
    redis_host = redis_host=  sys.argv[2]

    client = KafkaClient(zookeeper_hosts=zkKafka)
    topic = client.topics['Throughput']
    consumer = topic.get_simple_consumer(reset_offset_on_start=True)
    r = redis.StrictRedis(host=redis_host, port=6379, db=0)

    centroids = get_clusters()
    dist = calculate_distances(elements,centroids)
    partial_sums = find_partial_sums(dist,centroids)
    save_sums_to_redis()
