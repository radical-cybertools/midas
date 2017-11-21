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

def get_data_from_kafka(kafka_messages, window, output_queue,alock):

    while True:
        start_consumption = time.time()
        Nmessages = 0
        data = []
        while time.time() - start_consumption < window:
            message = consumer.consume(block=True)
            data_np = np.array(ast.literal_eval(message.value))
            data.append(data_np)
            Nmessages+=1  # save that
        # unlock to send
        alock.acquire()
        output_queue.put(data)
        alock.release()


    return

### return a np.array of the elments
def processing_process(data_batches,alock):

    def elements_of_consumed_batch(input_queue,alock):

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
        #ncentroids = centroids.shape[0]
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

    while True:  # infinite loop because it is a streaming application
        centroids = get_clusters()
        alock.acquire()
        elements = elements_of_consumed_batch(data_batches)
        dist = calculate_distances(elements, centroids)
        partial_sums = find_partial_sums(dist, centroids, elements)
        save_sums_to_redis(partial_sums)
        r.set('status', 'True') # change status to move to reduce step
        
        check_status = r.get('status'):  # when status is False again clusters are updated
        while check_status==True:        # so the system can get the new data
            pass
        alock.release()   #status works with one map CU - FIXME: to work with multiple 

    return



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
    alock = mp.lock()
    processes = [mp.Process(target=get_data_from_kafka, args=(window,data_batches,alock)), 
            mp.Process(target=processing, args=(data_batches,alock)]   #TODO: fix this

    
    # Run processes
    for p in processes:
        p.start()


    for p in processes:
        p.join()
