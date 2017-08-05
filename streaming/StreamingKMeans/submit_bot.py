#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import os
import radical.pilot as rp
import numpy as np

#os.environ['RADICAL_PILOT_PROFILER']= 'TRUE'
os.environ['RADICAL_PILOT_VERBOSE']= 'DEBUG'

""" DESCRIPTION: Tutorial 1: A Simple Workload consisting of a Bag-of-Tasks
"""

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!


#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        sys.exit (1)


#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    if not unit:
        return

    global CNT

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot, state)

    if state == rp.FAILED:
        print "stderr: %s" % unit.stderr
        sys.exit(2)


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    session = rp.Session()
    print "session id: %s" % session.uid

    c = rp.Context('ssh')
    c.user_id = "tg829618"
    session.add_context(c)
    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' in the 'finally' clause.
    try:

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initializing Pilot Manager ..."
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        pmgr.register_callback(pilot_state_cb)

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # 
        # Change the resource below if you want to run on a remote resource. 
        # You also might have to set the 'project' to your allocation ID if 
        # your remote resource does compute time accounting. 
        #
        # A list of preconfigured resources can be found at: 
        # http://radicalpilot.readthedocs.org/en/latest/machconf.html#preconfigured-resources
        # 
        pdesc = rp.ComputePilotDescription ()
	
        pdesc.resource = "xsede.wrangler_streaming"  # this is a "label", not a hostname
        pdesc.cores    = 50
        pdesc.runtime  = 30  # minutes
        pdesc.cleanup  = False  # clean pilot sandbox and database entries
        #pdesc.project = 'TG-MCB090174'
        pdesc.project = 'TG-MCB090174:dssd+TG-MCB090174+2345'

        #pdesc.queue = 'development'
        #if pdesc.resource =='xsede.wrangler_streaming':
        #    pdesc.queue = 'debug'

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        # create a UnitManager which schedules ComputeUnits over pilots.
        print "Initializing Unit Manager ..."
        umgr = rp.UnitManager (session=session)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)

        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)


        #----------------------------DUMP CU---------------------------------#
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable = 'python'
        cudesc.arguments = ['test.py']
        cudesc.input_staging = ['test.py']
        cudesc.cores = 1
        #---------------------------END DUMP CU------------------------------#
        cu_set = umgr.submit_units(cudesc)
        umgr.wait_units()

        #----------------- KAFKA SETTINGS -------------------------------------#
        pilot_info = pilot.as_dict()
        pilot_info = pilot_info['resource_details']['lm_detail']
        zk_url = pilot_info['zk_url']
        broker = pilot_info['brokers'][0] + ':9092'
        TOPIC_NAME = 'KmeansList'
        number_of_partitions = 48
        broker_string = ''
        brokers = pilot_info['brokers']
        print pilot_info

        print brokers

        for br in brokers:
            temp = br +':9092,'
            temp = str(temp)
            broker_string += temp

        print broker_string
        #---------------------------------------------------------------------#


        #----------BEGIN USER DEFINED KAFKA-CU DESCRIPTION-------------------#
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable = 'kafka-topics.sh'
        cudesc.arguments = [' --create --zookeeper %s  --replication-factor 1 --partitions %d \
                                --topic %s' % (zk_url,number_of_partitions,TOPIC_NAME)]
        cudesc.cores = 2
        #-----------END USER DEFINED KAFKA-CU DESCRIPTION--------------------#
        
        cu_set = umgr.submit_units(cudesc)
        umgr.wait_units()

        print "Creating a session"
        cudesc_list = []

        #--------BEGIN USER DEFINED KAFKA-producer--------------------------#
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable = 'python'
        cudesc.arguments = ['StreamingProducer.py',broker_string,TOPIC_NAME,number_of_partitions]
        cudesc.input_staging = ['StreamingProducer.py']
        cudesc.cores = 6
        #--------END USER DEFINED CU DESCRIPTION----------------------------#
        cudesc_list.append(cudesc)

        #--------BEGIN USER DEFINED SPARK-CU DESCRIPTION-------#
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable = "spark-submit"
        cudesc.arguments = ['--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 ',
                'StreamingKMeans.py', broker_string,TOPIC_NAME, number_of_partitions ,'--verbose ',
                ' --conf spark.eventLog.enabled=true ', '--conf spark.eventLog.dir=./ ']
	cudesc.input_staging = ['StreamingKMeans.py']
	cudesc.cores = 15
        #------------ END USER DEFINED DESCRPITION---------------------------#
        cudesc_list.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print "Submit Compute Units to Unit Manager ..."
        cu_set = umgr.submit_units (cudesc_list)

        print "Waiting for CUs to complete ..."
        umgr.wait_units()

        print "All CUs completed:"

    except Exception as e:
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print "need to exit now: %s" % e

    finally:
        print "closing session"
        session.close ()

#-------------------------------------------------------------------------------
