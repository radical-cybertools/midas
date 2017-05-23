from pykafka import KafkaClient
import os
import sys
import time
import dateutil
import dateutil.parser
import datetime

RESULT_FILE= "results/kafka-latency.csv"
try:
    os.makedirs("results")
except:
    pass
output_file=open(RESULT_FILE, "w")


broker = sys.argv[1]
topic_name = sys.argv[2]
client = KafkaClient(hosts=broker)
#print client.topics

topic = client.topics[topic_name]
consumer = topic.get_simple_consumer()
#print topic
#print consumer

start = time.time()

while True:  # consumer never stops 
    message = consumer.consume(block=True)
    now = time.time()
    sent_time_string = message.value.split(";")[0]
    sleep_time =float(message.value.split(";")[1])
    sent_time = dateutil.parser.parse(sent_time_string)
    sent_time_ts = time.mktime(sent_time.timetuple())
    lat = now-sent_time_ts
    
    result = "kafka, latency, 0, %.5f, %.5f, %s, %s\n"%(1/sleep_time, lat, message.value.split(";")[0], 
                                                                                datetime.datetime.now().isoformat())
    output_file.write(result)
    output_file.flush()
    sys.stdout.flush()
