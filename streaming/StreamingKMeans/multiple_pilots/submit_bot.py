#!/usr/bin/env python
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys, os
import radical.pilot as rp
import radical.utils as ru
import time
import numpy as np

#os.environ['RADICAL_PILOT_PROFILER']= 'TRUE'
os.environ['RADICAL_PILOT_VERBOSE']= 'DEBUG'

#
if __name__ == "__main__":

    session = rp.Session()
    print "session id: %s" % session.uid
    broker_string = sys.argv[1] + ':9092'

    try:

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initializing Pilot Manager ..."
        pmgr = rp.PilotManager(session=session)
        #pmgr.register_callback(pilot_state_cb)

        pdesc = rp.ComputePilotDescription()
        pdesc.resource = "xsede.wrangler"  # this is a "label", not a hostname
        pdesc.cores    = 48
        pdesc.runtime  = 45  # minutes
        pdesc.cleanup  = False  # clean pilot sandbox and database entries
        pdesc.project = 'TG-MCB090174'
        #pdesc.project = 'TG-MCB090174:dssd+TG-MCB090174+2432' 
        pdesc.access_schema = 'gsissh'

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        # create a UnitManager which schedules ComputeUnits over pilots.
        print "Initializing Unit Manager ..."
        umgr = rp.UnitManager (session=session)
        #umgr.register_callback(unit_state_cb)

        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        print "Creating a session"
        ## ------ EXPERIMENTAL CONFIGURATIONS------------------------------------------#
        NUMBER_OF_PRODUCERS = 5
       #--------------------------------------------------------------------------------

        cudesc_list =[]
        for producer_id in xrange(NUMBER_OF_PRODUCERS):
            #--------KAFKA-producer--------------------------#
            cudesc = rp.ComputeUnitDescription()
            cudesc.executable = 'python'
            cudesc.arguments = ['data_producer.py',broker_string]
            cudesc.input_staging = ['data_producer.py']
            cudesc.cores = 1
            umgr.submit_units([cudesc])
            umgr.submit_units([cudesc])
            time.sleep(80)
            #cudesc_list.append(cudesc)
            
            #--------END USER DEFINED CU DESCRIPTION----------------------------#

        print "Submit Compute Units to Unit Manager ..."
        #cu_set = umgr.submit_units(cudesc_list)

        print "Waiting for CUs to complete ..."
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
        session.close ()
