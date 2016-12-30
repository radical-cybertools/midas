#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import radical.pilot as rp
import copy
import networkx as nx
import MDAnalysis as mda
import numpy as np
from datetime import datetime


#SHARED_INPUT_FILE = 'input.txt'
MY_STAGING_AREA = 'staging:///'
os.environ['RADICAL_PILOT_DBURL'] = "mongodb://sean:1234@ds019678.mlab.com:19678/pilot_test"

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

        # Read the number of the divisions you want to create
    args = sys.argv[1:]
    if len(args) < 3:
        print "Usage: "
        print "python leaflet_finder.py  <window size> #cores <uni filename> <traj filename> <report file name>"
        sys.exit(-1)
    WINDOW_SIZE = int(sys.argv[1])
    CPUs = int(sys.argv[2]) # number of cores
    coord_numpy_file=sys.argv[3]
    #uni_filename=sys.argv[3]
    #traj_filename = sys.argv[4]
    #report_name = sys.argv[5]

    #try:
    #    universe=mda.Universe(uni_filename, traj_filename)
    #except IOError:
    #    print "Missing data-set files! Check the name of the dataset"
    #    sys.exit(-1)

    #selection = universe.select_atoms('name P*')
    coord = np.load(coord_numpy_file)
    data=open('input.txt','w')
    #for i in range(0,100):
    for coordinates in coord:
        data.write('%s,%s,%s\n'%(coordinates.tolist()[0],coordinates.tolist()[1],coordinates.tolist()[2]))
        #data.write('%s,%s,%s\n'%(coord[i].tolist()[0],coord[i].tolist()[1],coord[i].tolist()[2]))

    data.close()

    total_file_lines = coord.shape[0]
    cutoff=15.0

    try:
        # Create a new session.
        session = rp.Session()
        print "session id: %s" % session.uid
        c = rp.Context('ssh')
        c.user_id = "solejar"
        session.add_context(c)

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        print "Initializing Pilot Manager ..."
        pmgr = rp.PilotManager(session=session)

        pmgr.register_callback(pilot_state_cb)

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # 
        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = "xsede.comet" # NOTE: This is a "label", not a hostname
        pdesc.runtime  = 80 # minutes
        pdesc.cores    = CPUs
        pdesc.cleanup  = False
        #pdesc.queue    = 'development'
        pdesc.project  = 'unc100'

        # submit the pilot.
        print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        cu_full_list=list()

        # Define the url of the local file in the local directory
        shared_input_file_url = 'file://%s/%s' % (os.getcwd(), 'input.txt')
        staged_file = "%s%s" % (MY_STAGING_AREA, 'input.txt')

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
                     'target': 'input.txt',
                     'action': rp.LINK
        }

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        print "Initializing Unit Manager ..."
        umgr = rp.UnitManager (session=session,
                               scheduler=rp.SCHED_DIRECT)

        umgr.register_callback(unit_state_cb)
        # Add the created ComputePilot to the UnitManager.
        print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        NUMBER_OF_TRAJECTORIES = total_file_lines
         # i should define a "good" window based on division rules. in respect to the NUMBER_OF_TRAJECTORIES
        

        # submit CUs to pilot job
        cudesc_list = []
        sd_inter_in_list = list()
        #print 'window is ' + str(WINDOW_SIZE)
        #print 'traj count is ' + str(NUMBER_OF_TRAJECTORIES)
        for i in range(1,NUMBER_OF_TRAJECTORIES+1,WINDOW_SIZE):
            for j in range(i,NUMBER_OF_TRAJECTORIES,WINDOW_SIZE):
                    # I save here the results of each cu (matrix of all pairs) and stage it to create the graph
                INTERMEDIATE_FILE = "distances_%d_%d.npz.npy" % (i-1,j-1)
                sd_inter_out = {
                'source': INTERMEDIATE_FILE,
                # Note the triple slash, because of URL peculiarities
                'target': 'staging:///%s' % INTERMEDIATE_FILE,
                'action': rp.COPY
                }
                # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
                cudesc = rp.ComputeUnitDescription()
                cudesc.name='Euclidean_dist_%d_%d'%(i,j)
                cudesc.pre_exec=['module load python/2.7.10']
                cudesc.executable  = "python"
                cudesc.cores = 4
                cudesc.arguments   = ['atom_distances.py', WINDOW_SIZE, i, j, total_file_lines,cutoff] # each CU should compute window size distances 
                                                                        # I use i to calculate from which element I start calculating distances in each cu
                cudesc.input_staging = ['atom_distances.py',sd_shared]
                cudesc.output_staging = [sd_inter_out]
                # -------- END USER DEFINED CU DESCRIPTION --------- #
                #print 'i, j : ' + str(i) + str(j)
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
        cu_full_list.extend(cu_set)
        print "Waiting for CUs to complete ..."
        umgr.wait_units()
        print "All CUs completed successfully!"
    
        # Create and submit a task to the pilot. This CU is going to aggregate the results of  the previous tasks
        # create a big graph and find all the connected components of the graph
        sd_inter_in_list.append('find_connencted_components.py')
        # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
        cudesc = rp.ComputeUnitDescription()
        cudesc.name='ConnComp'
        cudesc.executable = "python"
        cudesc.pre_exec=['module load python/2.7.10','source /home/solejar/radical.pilot.sandbox/ve_comet/bin/activate']
        #cudesc.pre_exec = ['module load python/'] # futuregrid machine 
        cudesc.arguments = ['find_connencted_components.py',NUMBER_OF_TRAJECTORIES,WINDOW_SIZE]
        cudesc.input_staging = sd_inter_in_list
        cudesc.output_staging = [{'source':'components.npz.npy',
                                  'target':'components.npz.npy',
                                  'action':rp.TRANSFER}]
        # -------- END USER DEFINED CU DESCRIPTION --------- #
        print "Submit subgraph task"
        cuset = umgr.submit_units(cudesc)
        umgr.wait_units()
        print "Task completed successfully!"

        cu_full_list.append(cuset)
        #How the output should be

        ## Input topology/trajectory: md_prod_12x12_lastframe.pdb md_prod_12x12_everymicroS_pbcmolcenter.xtc
        ## Head group atom selection: name P*
        ## Number of lipids:          39024
        ## Number of phosphates (P):  44784
        ## [time(ps)] [N_leaflets] [N_lipids for the top 5 leaflets]
        #1000000.0   2 21456 17568
        #2000000.0   3 21455 17568     1
        #3000000.0   6 21449 17567     4     2     1
        #4000000.0  12 21448 17534    30     2     2
        #5000000.0  10 21451 17562     2     3     1
        #6000000.0   4 21452 17567     4     1
        #7000000.0  15 21450 17458    87    15     3
        #8000000.0  12 21451 17561     2     2     1
        #9000000.0  18 21446 17439   114     6     3
        #10000000.0  11 21441 17396   168     6     5
        #11000000.0   9 21454 17563     1     1     1



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
