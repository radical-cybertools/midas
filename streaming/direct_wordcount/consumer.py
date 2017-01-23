from pykafka import KafkaClient
import sys




broker = sys.argv[1]

print broker


client = KafkaClient(hosts=broker)
print client.topics

topic = client.topics['KmeansList']
consumer = topic.get_simple_consumer()

for message in consumer:
    if message is not None:
        print message.offset, message.value
    else:
        print 'None'


while True:
    message = consumer.consume(block=True)
    #print message.value
    now = time.time()
    #sent_time=datetime.datetime.strptime(message.value, "%Y-%m-%dT%H:%M:%S.%fZ")
    sent_time_string = message.value.split(";")[0]
    sleep_time =float(message.value.split(";")[1])
    sent_time = dateutil.parser.parse(sent_time_string)
    sent_time_ts = time.mktime(sent_time.timetuple())
    lat = now-sent_time_ts - TIME_OFFSET   
    result = "kafka, latency, 0, %.5f, %.5f, %s, %s\n"%(1/sleep_time, lat, message.value.split(";")[0], 
                                                                                datetime.datetime.now().isoformat())
    print(result)
