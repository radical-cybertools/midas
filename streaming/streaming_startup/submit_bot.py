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
    #c.user_id = "georgeha"

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
        pdesc.cores    = 32
        pdesc.runtime  = 8  # minutes
        pdesc.cleanup  = True  # clean pilot sandbox and database entries
        pdesc.project = "TG-MCB090174"
        pdesc.queue = 'development'
        #pdesc.queue = 'debug'

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        # create a UnitManager which schedules ComputeUnits over pilots.
        print "Initializing Unit Manager ..."
        umgr = rp.UnitManager (session=session,)
                               #scheduler=rp.SCHED_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        umgr.register_callback(unit_state_cb)

        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        NUMBER_JOBS  = 1 # the total number of cus to run
        pilot_info = pilot.as_dict()



        # create CU descriptions
        #--------BEGIN USER DEFINED SPARK-CU DESCRIPTION-------#
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable = "spark-submit"
        cudesc.arguments = ['--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,\
                             pi.py   20  ', '--verbose']
        cudesc.input_staging = ['pi.py'] 
        cudesc.cores = 20
        #---------END USER DEFINED CU DESCRIPTION---------------#

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print "Submit Compute Units to Unit Manager ..."
        cu_set = umgr.submit_units (cudesc)

        print "Waiting for CUs to complete ..."
        umgr.wait_units()

        print pilot.as_dict()
        print "All CUs completed:"

        info = pilot.as_dict()
        info = info['resource_detail']['lm_detail']

        print 'Spark download   ,    Spark startup   ,   kafka download      ,      kafka startup, nodes,  system'
        print ' %s        ,     %s        ,    %s          ,    %s     , %s   , stampede' \
        % (info['spark_download'], info['cluster_startup'], info['zk_download'], info['zk_startup'], str(pdesc.cores/16))

        a_str = '%s        ,     %s        ,    %s          ,    %s     , %s   ,stampede\n' \
        % (info['spark_download'], info['cluster_startup'], info['zk_download'], info['zk_startup'], str(pdesc.cores/16))

        #fo = open('/home/georgeha/repos/midas_exps/streaming/rp-streaming_startup.csv','a')
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
        # always clean up the session, no matter if we caught an exception or
        # not.
        print "closing session"
        session.close ()

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots.




#-------------------------------------------------------------------------------
