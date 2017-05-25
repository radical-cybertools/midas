from pykafka import KafkaClient
from pykafka.partitioners import  hashing_partitioner
from time import time
import sys



'''
Producer is creating messages of equal size (100)


'''

if __name__ == "__main__":


    broker = sys.argv[1]
    number_of_points = int(sys.argv[2])
    broker2 = sys.argv[3]
    brokers = str(broker)  + ',' + broker2
    client = KafkaClient(hosts=brokers)
    topic = client.topics['KmeansList']

    ttc = time()
    count = 0
    size = 20
    
    n_of_messages = number_of_points / size
    elements = list()

    for j in xrange(size):
        elements.append(j)
    
    el_str = str(elements)


    with topic.get_sync_producer(partitioner=hashing_partitioner) as producer:
        for i in xrange(n_of_messages):
            producer.produce(el_str, partition_key='{}'.format((i % 2) + 1 ))
    ttc = time() - ttc

    print ttc
