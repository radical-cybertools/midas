import os, subprocess

broker = 'c251-136:9092'
zk = ' c251-119:2181'
spark = ' c251-119:7077'


kafka_home = os.getcwd() + '/kafka_2.11-0.8.2.1'
spark_home = os.getcwd() + '/spark-2.2.0-bin-hadoop2.7'
source_files = os.getcwd() + '/midas/streaming/StreamingKMeans/multiple_pilots'
topic = 'Throughput'
number_of_partitions = 48*16  # set one partition per core


#check if a topic already exists
topics = os.path.join(kafka_home, 'bin/kafka-topics.sh') + ' --list --zookeeper {}'.format(zk)
#print topics
#subprocess.check_output(topics.split())


create_topic = '%s/bin/kafka-topics.sh --create --zookeeper %s  --replication-factor 1 --partitions %d --topic %s' % (kafka_home,zk,number_of_partitions,topic)
#create_topic
#subprocess.check_output(create_topic.split())


## create spark command
#spark_command =os.path.join(spark_home,'bin/spark-submit') + ' --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 ' + os.path.join(source_files,'StreamingKMeans.py')  + ' ' + broker  + ' ' + topic + ' ' + str(number_of_partitions) + ' --verbose ' + ' --conf spark.eventLog.enabled=true ' + ' --conf spark.eventLog.dir=./csv_logs '

#spark_command += '  --conf spark.driver.maxResultSize=5g --executor-memory 40g --driver-memory  20g'

spark_command =  os.path.join(spark_home,'bin/spark-submit') + ' --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 ' 

spark_command += ' --verbose '  + ' --conf spark.eventLog.enabled=true ' + ' --conf spark.eventLog.dir=./csv_logs '

spark_command += '  --conf spark.driver.maxResultSize=5g --executor-memory 40g --driver-memory  20g  '

spark_command +=  os.path.join(source_files,'StreamingKMeans.py')  + ' ' + broker  + ' ' + topic + ' ' + str(number_of_partitions) 

#print spark_command

spark_command =  os.path.join(spark_home,'bin/spark-submit') + '  ' + os.path.join(spark_home,'examples/src/main/python/pi.py')
subprocess.check_output(spark_command.split())
print spark_command

