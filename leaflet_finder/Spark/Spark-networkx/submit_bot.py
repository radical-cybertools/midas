#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import sys
import os

os.environ['RADICAL_PILOT_VERBOSE'] = 'DEBUG'

import radical.pilot as rp
import argparse
#import numpy as np



""" DESCRIPTION: Tutorial 1: A Simple Workload consisting of a Bag-of-Tasks
"""

# READ: The RADICAL-Pilot documentation: 
#   http://radicalpilot.readthedocs.org/en/latest
#
# Try running this example with RADICAL_PILOT_VERBOSE=debug set if 
# you want to see what happens behind the scences!

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("win_size", help="Dimension size of the submatrix")
    parser.add_argument("cores", help="Number of cores that will be requested")
    #parser.add_argument("uni", help="Universe File, extension tpr")
    #parser.add_argument("trajs", help="Trajectory File, extension xtc")
    parser.add_argument("atomFile",help="The numpy file that contains the positions of the atoms")
    parser.add_argument("session", help="The RADICAL-Pilot Session name")
    args = parser.parse_args()
    
    partitions = int(args.win_size)
    CPUs = int(args.cores) # number of cores
    session_name = args.session


    #uni_filename = args.uni
    #traj_filename = args.trajs

    #try:
    #    universe=mda.Universe(uni_filename, traj_filename)
    #except IOError:
    #    print "Missing universe and trajectory file"
    #    sys.exit(-1)
    #selection = universe.select_atoms('name P*')

    # Numpy file that contains the atom positions
    atom_file_name = args.atomFile
    #np.save(atom_file_name,selection.positions)


    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session(database_url=os.environ.get('RADICAL_PILOT_DBURL'),name = session_name)
    print "session id: %s" % session.uid

    c = rp.Context('ssh')
    c.user_id = "iparask"
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
        # pmgr.register_callback(pilot_state_cb)

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
        pdesc.resource = "xsede.comet_spark"  # this is a "label", not a hostname
        pdesc.cores    = cores
        pdesc.runtime  = 60  # minutes
        pdesc.cleanup  = False  # clean pilot sandbox and database entries
        pdesc.project = "unc100"
        #pdesc.queue = 'development'

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
        #umgr.register_callback(unit_state_cb)

        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        NUMBER_JOBS  = 1 # the total number of cus to run

        # create CU descriptions
        cudesc_list = []
        for i in range(NUMBER_JOBS):

            # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
            cudesc = rp.ComputeUnitDescription()
            #cudesc.pre_exec=['export PYSPARK_PYTHON=/home/iparask/radical.pilot.sandbox/ve_comet/bin/python']
            cudesc.executable  = "spark-submit"
            cudesc.arguments =  ['--conf','spark.eventLog.enabled=true',\
                                 '--conf','spark.eventLog.dir=./', \
                                 '--conf','spark.ui.port=4045',\
                                 '--conf spark.driver.maxResultSize=30g', \
                                 '--executor-memory 60g', \
                                 '--driver-memory 60g',  \
                                 'leafletfinder.py %d %s' % (partitions,atom_file_name)]
            cudesc.input_staging = ['leafletfinder.py', atom_file_name]
            cudesc.cores       = cores
            # -------- END USER DEFINED CU DESCRIPTION --------- #

            cudesc_list.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        # PilotManager. This will trigger the selected scheduler to start
        # assigning ComputeUnits to the ComputePilots.
        print "Submit Compute Units to Unit Manager ..."
        cu_set = umgr.submit_units (cudesc_list)

        print "Waiting for CUs to complete ..."
        umgr.wait_units()

        print "All CUs completed:"
        for unit in cu_set:
            print "* CU %s, state %s, exit code: %s, stdout: %s" \
                % (unit.uid, unit.state, unit.exit_code, unit.stdout.strip())
    

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
        print pilot.as_dict()
        print "closing session"
        session.close (cleanup=False)

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots.


#-------------------------------------------------------------------------------

