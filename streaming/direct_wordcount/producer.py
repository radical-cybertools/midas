from pykafka import KafkaClient
import sys





if __name__ == "__main__":


    zookeeper_host = sys.argv[1]  
    

    client = KafkaClient(hosts=zookeeper_host)
    print client.topics

    topic = client.topics['KmeansList']
    with topic.get_sync_producer() as producer:
        for i in xrange(100):
            print 'test'
            producer.produce(str('word number 1 \n'))

