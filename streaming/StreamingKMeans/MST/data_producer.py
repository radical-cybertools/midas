#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Sat Dec  9 15:05:49 2017

@author: Georgos Chantzialexiou
"""


from pykafka import KafkaClient
import numpy as np
import os, sys
import time
import datetime
from pykafka.partitioners import hashing_partitioner



def setup_kafka_producer(broker):

    global producer
    
    client = KafkaClient(hosts=broker)
    topic = client.topics[TOPIC_NAME]
    producer = topic.get_sync_producer(partitioner=hashing_partitioner)
    
    return producer
    
    
def setup_metrics():
    global stdout_file
    run_timestamp=datetime.datetime.now()
    run_timestamp = run_timestamp.strftime("%Y%m%d-%H%M%S")
    STDOUT_FILE = "producer-metrics" + run_timestamp + ".csv"
    stdout_file = open(STDOUT_FILE, 'w')
    stdout_file.write('Produce_batch_from,Produce_batch_until,\
        Num_Messages,Number_of_points_per_Message,\
        Number_Dim,Message_size_in_Bytes,\
        APoints/Msg,KB_Transfered,KB/sec,TimeStamp\n')
    
    return


def get_random_cluster_points(number_points, number_dim):
    """
    random number generator ( of 1 message)
    input: (int,int)
    output: type = numpy.ndarray, shape = number_points, number_dim
    """
    mu = np.random.randn()
    sigma = np.random.randn()
    p = sigma * np.random.randn(number_points, number_dim) + mu
    
    return p


def generate_messages_and_save_them_to_np_array(Number_of_messages,\
                                                number_points_per_message, dim):
    """
    Generation of all the messages
    input: (int,int,int)
    output: type = (numpy.ndarray, int)
            shape = (Number_of_messages*number_points_per_message,dim)
    """
    points = []
    for i in xrange(Number_of_messages):
        p = get_random_cluster_points(number_points_per_message, dim)
        points.append(p)
        
    points_np = np.concatenate(points)
    #number_batches = points_np.shape[0]/number_points_per_messages
    
    return points_np
    


def publish_messages(msg_number, msg_np):
    
    run_timestamp=datetime.datetime.now()
    ts = run_timestamp.strftime("%Y%m%d-%H%M%S")
    last_index = msg_number*MESSAGE_OF_POINTS_PER_MESSAGE
    points_batch = points_np[last_index:last_index+MESSAGE_OF_POINTS_PER_MESSAGE]
    points_strlist=str(points_batch.tolist())
    message_size_in_bytes =  len(points_strlist)/1024  # in bytes
    tbytes = bytes + len(points_strlist)
    producer.produce(points_strlist, partition_key='{}'.format(msg_number))
    
    stdout_file.write("%d,%d,%d,%d,%d,%d, %.1f,%s,%s\n"%\
                                    (last_index,                                                                                           
                                     last_index+MESSAGE_OF_POINTS_PER_MESSAGE, 
                                     msg_number,
                                     MESSAGE_OF_POINTS_PER_MESSAGE,
                                     NUMBER_DIM,
                                     message_size_in_bytes,
                                     tbytes/1024,
                                     tbytes/1024/(time.time()-global_start),
                                     ts))
    stdout_file.flush()

    return
            
                
                #tbytes = bytes + len(points_strlist)
            
            #end = time.time()
            



    

if __name__ == '__main__':
    
    NUMBER_OF_MESSAGES =  10000
    NUMBER_OF_DIM = 3
    MESSAGE_OF_POINTS_PER_MESSAGE = 5000
    
    #broker = sys.argv[1]
    #producer = setup_kafka_producer(broker)
    #setup_metrics()
    tbytes = 0
    
    
    global_start = time.time()
    #msg_list = generate_messages_and_save_them_to_np_array(NUMBER_OF_MESSAGES,\
    #                             MESSAGE_OF_POINTS_PER_MESSAGE, NUMBER_OF_DIM)
    
    
    #for i in xrange(NUMBER_OF_MESSAGES):
    #    publish_messages(msg_number=i,msg_np=msg_list)
        
        
        
    
    
    
    
    
    
    


    

    