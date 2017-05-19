from pykafka import KafkaClient
from pykafka.partitioners import  hashing_partitioner
import datetime
import time
import sys





if __name__ == "__main__":


    broker = sys.argv[1]
    client = KafkaClient(hosts=broker)
    topic = client.topics['KmeansList']

    ttc = time.time()
    producer = topic.get_sync_producer()


    for sleep_time in [1, 0.1, 0.01, 0.001, 0.0001, 0.00001]:
        for i in range(5):
            message = datetime.datetime.now().isoformat() + ";" + str(sleep_time)
            message = str(message)
            message_2 = 'test'
            #producer.produce(message)
            #producer.produce(message_2)
            producer.produce('Hello world')
            
            time.sleep(sleep_time)

    ttc = time.time() - ttc

    print ttc


