from pykafka import KafkaClient
from pykafka.partitioners import  hashing_partitioner
from time import time
import sys





if __name__ == "__main__":


    broker = sys.argv[1]
    number_of_points = int(sys.argv[2])
    broker2 = sys.argv[3]
    brokers = str(broker)  + ',' + broker2
    client = KafkaClient(hosts=broker)
    topic = client.topics['KmeansList']

    ttc = time()
    count = 0
    size = 100
    if number_of_points>-100000:
        size = 1000
    batch_size = number_of_points / size   # batch_size 100 then for 100.000K el 1000
    elements = list()
    for j in xrange(batch_size):
        elements.append(j)
    
    el_str = str(elements)


    with topic.get_sync_producer(partitioner=hashing_partitioner) as producer:
        for i in xrange(size):
            producer.produce(el_str, partition_key='{}'.format(i))
#            print 'test'
    ttc = time() - ttc

    print ttc
