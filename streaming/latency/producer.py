from pykafka import KafkaClient
from pykafka.partitioners import  hashing_partitioner
import datetime
import time
import sys





if __name__ == "__main__":


    broker = sys.argv[1]
    topic_name = sys.argv[2]
    client = KafkaClient(hosts=broker)
    topic = client.topics[topic_name]

    producer = topic.get_sync_producer()


    for sleep_time in [1, 0.1, 0.01, 0.001, 0.0001, 0.00001]:
        for i in xrange(100):
            message = datetime.datetime.now().isoformat() + ";" + str(sleep_time)
            producer.produce(message)
            time.sleep(sleep_time)



    print 'Messages produced'


