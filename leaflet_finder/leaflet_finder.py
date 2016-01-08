#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import radical.pilot as rp
import copy
import networkx as nx


SHARED_INPUT_FILE = 'input.txt'
MY_STAGING_AREA = 'staging:///'

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

    args = sys.argv[1:]
    if len(args) < 1:
        cutoff = 16 #default value
    else:
        cutoff = int(sys.argv[1])  # cutoff value of edges between atoms

    try:
        data = open(SHARED_INPUT_FILE,'r')
    except IOError:
        print "Missing data-set. file! Check the name of the dataset"
        sys.exit(-1)
    total_file_lines =  sum(1 for _ in data)
    data.close()

    # Create a new session.
    session = rp.Session()
    print "session id: %s" % session.uid

    try:

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        #c = rp.Context('ssh')
        #c.user_id = "username"
        #session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initializing Pilot Manager ..."
        pmgr = rp.PilotManager(session=session)

        pmgr.register_callback(pilot_state_cb)

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # 
        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = "local.localhost" # NOTE: This is a "label", not a hostname
        pdesc.runtime  = 10 # minutes
        pdesc.cores    = 2
        #pdesc.cleanup  = True

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        # Define the url of the local file in the local directory
        shared_input_file_url = 'file://%s/%s' % (os.getcwd(), SHARED_INPUT_FILE)
        staged_file = "%s%s" % (MY_STAGING_AREA, SHARED_INPUT_FILE)

         # Configure the staging directive for to insert the shared file into
        # the pilot staging directory.
        sd_pilot = {'source': shared_input_file_url,
                    'target': staged_file,
                    'action': rp.TRANSFER
        }
        # Synchronously stage the data to the pilot
        pilot.stage_in(sd_pilot)

        # Configure the staging directive for shared input file.
        sd_shared = {'source': staged_file, 
                     'target': SHARED_INPUT_FILE,
                     'action': rp.LINK
        }

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        print "Initializing Unit Manager ..."
        umgr = rp.UnitManager (session=session,
                               scheduler=rp.SCHED_BACKFILLING)

        umgr.register_callback(unit_state_cb)
        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        NUMBER_OF_TRAJECTORIES = total_file_lines
        WINDOW_SIZE = 3  # i should define a "good" window based on division rules. in respect to the NUMBER_OF_TRAJECTORIES
        

        # submit CUs to pilot job
        cudesc_list = []
        sd_inter_in_list = list()
        for i in range(1,NUMBER_OF_TRAJECTORIES+1,WINDOW_SIZE):
            for j in range(i,NUMBER_OF_TRAJECTORIES,WINDOW_SIZE):
                    # I save here the results of each cu (matrix of all pairs) and stage it to create the graph
                INTERMEDIATE_FILE = "distances_%d_%d.data" % (i-1,j-1)
                sd_inter_out = {
                'source': INTERMEDIATE_FILE,
                # Note the triple slash, because of URL peculiarities
                'target': 'staging:///%s' % INTERMEDIATE_FILE,
                'action': rp.COPY
                }
                # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
                cudesc = rp.ComputeUnitDescription()
                cudesc.executable  = "python"
                cudesc.arguments   = ['atom_distances.py', WINDOW_SIZE, i, j, total_file_lines,cutoff] # each CU should compute window size distances 
                                                                        # I use i to calculate from which element I start calculating distances in each cu
                cudesc.input_staging = ['atom_distances.py',sd_shared]
                cudesc.output_staging = [sd_inter_out]
                # -------- END USER DEFINED CU DESCRIPTION --------- #
                cudesc_list.append(cudesc)

                # Configure the staging directive for input intermediate data - I will use it later
                sd_inter_in = {
                    # Note the triple slash, because of URL peculiarities
                    'source': 'staging:///%s' % INTERMEDIATE_FILE,
                    'target': INTERMEDIATE_FILE,
                    'action': rp.LINK
                }
                sd_inter_in_list.append(sd_inter_in)

        # Submit the previously created ComputeUnit descriptions to the
        print "Submit Compute Units to Unit Manager ..."
        cu_set = umgr.submit_units (cudesc_list)
        print "Waiting for CUs to complete ..."
        umgr.wait_units()
        print "All CUs completed successfully!"
    
        # Create and submit a task to the pilot. This CU is going to aggregate the results of  the previous tasks
        # create a big graph and find all the connected components of the graph
        sd_inter_in_list.append('find_connencted_components.py')
        # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable = "python"
        #cudesc.pre_exec = ['module load python'] # futuregrid machine 
        cudesc.arguments = ['find_connencted_components.py',NUMBER_OF_TRAJECTORIES,WINDOW_SIZE]
        cudesc.input_staging = sd_inter_in_list
        # -------- END USER DEFINED CU DESCRIPTION --------- #
        print "Submit subgraph task"
        cuset = umgr.submit_units(cudesc)
        umgr.wait_units()
        print "Task completed successfully!"

        print "Results"
        print cuset.stdout


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
        print "Closing session"
        session.close ()

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


#-------------------------------------------------------------------------------
