from pykafka import KafkaClient
import sys





if __name__ == "__main__":


    broker = sys.argv[1]  
    print broker
    

    client = KafkaClient(hosts=broker)
    print client.topics

    topic = client.topics['KmeansList']
    with topic.get_sync_producer() as producer:
        for i in xrange(10000):
            producer.produce('This is number %d \n' % i)

