from pykafka import KafkaClient
from pykafka.partitioners import  hashing_partitioner
from time import time
import numpy as np
import sys


if __name__ == "__main__":


    broker = sys.argv[1]
    number_of_points = int(sys.argv[2])
    broker2 = sys.argv[3]
    brokers = str(broker)  + ',' + broker2
    client = KafkaClient(hosts=broker)
    topic = client.topics['atoms']
    atoms = np.load('atom_pos_132K.npy')


    ttc = time()
    count = 0
    batch_size = 10

'''
    if number_of_points>=100000:
        size = 1000
    batch_size = number_of_points / size   # batch_size 100 then for 100.000K el 1000
    elements = list()
    for j in xrange(batch_size):
        elements.append(j)
    
el_str = str(elements)
'''

    with topic.get_sync_producer(partitioner=hashing_partitioner) as producer:
        for i in range(0,len(atoms),batch_size):
            start = i
            end = i + batch_size
            el_str = str(atoms[start:end])  # creating the message
            producer.produce(el_str, partition_key='{}'.format(i))

    ttc = time() - ttc

    print ttc
