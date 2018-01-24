#!/usr/bin/env python
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys, os
import radical.pilot as rp
import radical.utils as ru
import numpy as np
import time
#os.environ['RADICAL_PILOT_PROFILER']= 'TRUE'
os.environ['RADICAL_PILOT_VERBOSE']= 'DEBUG'

#
if __name__ == "__main__":

    if len(sys.argv)==1:
        print 'Usage: <broker> <zkKafka> <redis>'

    session = rp.Session()
    print "session id: %s" % session.uid
    broker_string = sys.argv[1] + ':9092'
    zk_kafka = sys.argv[2] + ':2181'
    redis_hostname = sys.argv[3]

    try:

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initializing Pilot Manager ..."
        pmgr = rp.PilotManager(session=session)
        #pmgr.register_callback(pilot_state_cb)

        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "xsede.wrangler"  # this is a "label", not a hostname
        pdesc.cores    = 48
        pdesc.runtime  = 20  # minutes
        pdesc.cleanup  = False  # clean pilot sandbox and database entries
        pdesc.project = 'TG-MCB090174'
        #pdesc.project = 'TG-MCB090174:dssd+TG-MCB090174+2397'
        pdesc.access_schema = 'gsissh'

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        # create a UnitManager which schedules ComputeUnits over pilots.
        print "Initializing Unit Manager ..."
        umgr = rp.UnitManager (session=session)

        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        print "Creating a session"

        print 'Setting up on redis kmeans model'

        ##----- create the kmeans model on redis ----------------#
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable = 'python'
        cudesc.arguments = ['setup_kmeans_model.py',redis_hostname]
        cudesc.input_staging = ['setup_kmeans_model.py']
        cudesc.cores =1
        ##------- -----------------------------------------------#
        cu_set = umgr.submit_units(cudesc)
        print 'kmeans model was created on redis sucessfuly'


        ## ------ EXPERIMENTAL CONFIGURATIONS------------------------------------------#
        NUMBER_OF_PRODUCERS = 2    # producer cus
        number_messages = 1000  #TODO: fix the number of messages 
        number_of_mappers = 2  # map-consumer CUs
        number_of_reducers = 1 # reduce-consumer CUs
        number_cus =1
        per_cu_messages = number_messages/number_cus
        Partitions = 48
        #--------------------------------------------------------------------------------
        filename = 'RpKmeansStreaming.csv'
        afile = open(filename,'a')
        afile.write('Action,Time,Partitions\n')
        it = 3
        while it>=0:
            it -=1
            print ' Creating the producer CUS..'
            cudesc_list =[]
            for producer_id in xrange(NUMBER_OF_PRODUCERS):
                #--------KAFKA-producer--------------------------#
                cudesc = rp.ComputeUnitDescription()
                cudesc.executable = 'python'
                cudesc.arguments = ['data_producer.py',broker_string,producer_id]
                cudesc.input_staging = ['data_producer.py']
 #               cudesc.output_staging = ['producer_data_%d.csv'%producer_id]
                cudesc.cores = 1   
                cudesc_list.append(cudesc)
                #--------END USER DEFINED CU DESCRIPTION----------------------------#

            print 'Defining the map-consumer CUs..'

            for mapper_id in xrange(number_of_mappers):
                cudesc = rp.ComputeUnitDescription()
                cudesc.executable  = "python"
                #cudesc.arguments   = ['mapper.py', per_cu_messages,i, number_of_consumers, \
                #                        zk_kafka, redis_hostname ]   # number of msg, <cu_id>, <total_number_cus> <zkKafka> <redis>
                cudesc.arguments = ['mapper.py',broker_string,redis_hostname,mapper_id,Partitions]
                cudesc.input_staging = ['mapper.py']
                cudesc.output_staging = ['mapper_data_%d.csv' % mapper_id]
                cudesc.cores       = 1
                cudesc_list.append(cudesc)


            print 'Submitting mapper and data producer'
            cu_set = umgr.submit_units(cudesc_list) #TODO: fix it to wait only the map-CU

            print "Waiting for mapper and data-producer CUs to complete ..."
            umgr.wait_units()

#            for producer_id in xrange(NUMBER_OF_PRODUCERS):
#                f = open('producer_data_%d.csv'%producer_id,'r')
#                temp = f.readline()
#                afile.write(temp.strip())
#                afile.flush()
#                f.close()
            
            for i in xrange(number_of_mappers):
                f = open('mapper_data_%d.csv' % i,'r')
                temp = f.readlines()
                afile.write(temp[0].strip())
                afile.write(temp[1].strip())
                afile.flush()
                f.close()


            print 'Defining the reduce-consumer CUs'
            cudesc_list = []
            for i in xrange(number_of_reducers):
                cudesc = rp.ComputeUnitDescription()
                cudesc.executable = 'python'
                cudesc.arguments = ['reducer.py',redis_hostname,i]
                cudesc.input_staging = ['reducer.py']
                cudesc.cores = 1
                cudesc_list.append(cudesc)

            start_time = time.time()
            print "Submit reduce=CU to Unit Manager ..."
            cu_set = umgr.submit_units(cudesc_list)
            end_reduce = time.time()
            #astring = 'Time to collect and update the centroids %f ' % (end_reduce - start_time)
            astring = 'Reduce, %f , %d' % (end_reduce - start_time, Partitions)
            afile.write(astring)



            print "Waiting for reduce-CUs to complete ..."
            umgr.wait_units()

            print "All CUs completed:"

    except Exception as e:
        print "caught Exception: %s" % e
        ru.print_exception_trace()
        raise
    except (KeyboardInterrupt, SystemExit) as e:
        print "need to exit now: %s" % e
        ru.print_exception_trace()
    finally:
        print "closing session"
        afile.close()
        print 'file closed'
        session.close ()
