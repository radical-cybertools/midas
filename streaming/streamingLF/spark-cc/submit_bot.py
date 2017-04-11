#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import os
import radical.pilot as rp
import numpy as np
import time

#os.environ['RADICAL_PILOT_DBURL']= 'mongodb://sean:1234@ds019678.mlab.com:19678/pilot_test'
#os.environ['RADICAL_PILOT_PROFILER']= 'TRUE'
os.environ['RADICAL_PILOT_VERBOSE']= 'DEBUG'

""" DESCRIPTION: Experiment 1: Kafka producer throughput
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

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

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
    #c.user_id = 'georgeha'
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
        pdesc.resource = "xsede.stampede_streaming"  # this is a "label", not a hostname
        pdesc.cores    = 16
        pdesc.runtime  = 17  # minutes
        pdesc.cleanup  = True  # clean pilot sandbox and database entries
        pdesc.project = "TG-MCB090174"
        #pdesc.project = 'unc100'
        pdesc.queue = 'development'

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        # create a UnitManager which schedules ComputeUnits over pilots.
        print "Initializing Unit Manager ..."
        umgr = rp.UnitManager (session=session,
                               scheduler=rp.SCHED_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)

        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        
        #----------BEGIN USER DEFINED TEST-CU DESCRIPTION-------------------#
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable = 'python'
        cudesc.arguments = ['test.py']
        cudesc.input_staging = ['test.py']
        cudesc.cores =1
        #-----------END USER DEFINED TEST-CU DESCRIPTION--------------------#
        cu_set = umgr.submit_units(cudesc)
        umgr.wait_units()

        #--------------- KAFKA SETTINGS ------------------------------------#
        number_of_partitions = 24
        number_of_points = 132*1000  # 132K scenario
        TOPIC_NAME = 'atoms'
        pilot_info = pilot.as_dict()
        pilot_info = pilot_info['resource_detail']['lm_detail']
        ZK_URL = pilot_info['zk_url']
        broker = pilot_info['brokers'][0] +'.stampede.tacc.utexas.edu' + ':9092'
        broker2 = 'empty broker'
        #broker2 = pilot_info['brokers'][1] + '.stampede.tacc.utexas.edu' + ':9092'
        print broker
        print broker2
        #-------------------------------------------------------------------#

        #----------BEGIN USER DEFINED KAFKA-CU DESCRIPTION-------------------#
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable = 'kafka-topics.sh'
        cudesc.arguments = [' --create --zookeeper %s  --replication-factor 1 --partitions %d \
                                --topic %s' % (ZK_URL,number_of_partitions,TOPIC_NAME)]
        cudesc.cores =2
        #-----------END USER DEFINED KAFKA-CU DESCRIPTION--------------------#
        cu_set = umgr.submit_units(cudesc)
        umgr.wait_units()

        cudesc_list = []

        #------BEGIN USER DEFINED PRODUCER-CU DESCRIPTION-----#
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable = "python"
        cudesc.arguments = ['producer.py',broker,number_of_points,broker2]
        cudesc.input_staging = ['producer.py']
        cudesc.cores = 10
        #---------END USER DEFINED CU DESCRIPTION---------------#

        cudesc_list.append(cudesc)

        #------BEGIN USER DEFINED CONSUMER-CU DESCRIPTION-----#
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable = "spark-submit"
        cudesc.arguments = ['--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.0 ',
                'streamingLF.py', broker,TOPIC_NAME, number_of_points,broker2]
        cudesc.input_staging = ['streamingLF.py']
        cudesc.cores = 10
        #---------END USER DEFINED CU DESCRIPTION---------------#
        
        cudesc_list.append(cudesc)

        cu_set = umgr.submit_units(cudesc_list)
        print 'Waiting for unit to complete'
        umgr.wait_units()
        
        print "All CUs completed:"
        for unit in cu_set:
        #unit = cu_set
        print "* CU %s, state %s, exit code: %s, stdout: %s" \
                 % (unit.uid, unit.state, unit.exit_code, unit.stdout)

        #Nodes,Number_of_points,Number_of_partitions,ttc,system
        ttc = unit.stdout
        ttc = ttc.strip()
        ## Save experiment data to csv file ##
        #fo = open('/home/georgeha/repos/midas_exps/streaming/add_name/.csv','a')
        #a_str = "%d,   %d   ,   %d   , %s, stampede\n" \
        #        % (pdesc.cores/16,number_of_points,number_of_partitions,ttc)
        #fo.write(a_str)
        #fo.close()


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
        session.close()
        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots.
