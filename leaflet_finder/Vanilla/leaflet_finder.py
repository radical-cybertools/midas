#!/usr/bin/env python

__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__   = "MIT"

import os
import sys
import argparse

os.environ['RADICAL_PILOT_VERBOSE'] = 'DEBUG'

import radical.pilot as rp
import copy
import MDAnalysis as mda
import numpy as np
from datetime import datetime


#SHARED_INPUT_FILE = 'input.txt'
MY_STAGING_AREA = 'staging:///'

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
    parser.add_argument("atomNum",help="The totatl number of atoms in the atomFile")
    parser.add_argument("session", help="The RADICAL-Pilot Session name")
    args = parser.parse_args()

    win_size = int(args.win_size)
    CPUs = int(args.cores) # number of cores
    #uni_filename = args.uni
    #traj_filename = args.trajs
    atom_file = args.atomFile
    session_name = args.session
    '''
    try:
        universe=mda.Universe(uni_filename, traj_filename)
    except IOError:
        print "Missing data-set files! Check the name of the dataset"
        sys.exit(-1)

    selection = universe.select_atoms('name P*')
    np.save('trajectories.npy',selection.positions)
    
    total_file_lines = selection.positions.shape[0]
    '''
    total_file_lines = args.atomNum
    cutoff=15.0

    try:
        # Create a new session.
        session = rp.Session(name=session_name)
        #print "session id: %s" % session.uid

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        #print "Initializing Pilot Manager ..."
        pmgr = rp.PilotManager(session=session)

        #pmgr.register_callback(pilot_state_cb)

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # 
        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = "xsede.comet" # NOTE: This is a "label", not a hostname
        pdesc.runtime  = 120 # minutes
        pdesc.cores    = CPUs
        pdesc.cleanup  = False
        pdesc.project  = 'unc100'

        # submit the pilot.
        #print "Submitting Compute Pilot to Pilot Manager ..."
        pilot = pmgr.submit_pilots(pdesc)

        cu_full_list=list()

        # Define the url of the local file in the local directory
        shared_input_file_url = 'file://%s/%s' % (os.getcwd(), 'trajectories.npy')
        #shared_input_file_url = 'file://%s/%s' % (os.getcwd(), atom_file)
        staged_file = "%s%s" % (MY_STAGING_AREA, 'trajectories.npy')

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
                     'target': 'trajectories.npy',
                     'action': rp.LINK
        }

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        #print "Initializing Unit Manager ..."
        umgr = rp.UnitManager (session=session,
                               scheduler=rp.SCHED_DIRECT)

        #umgr.register_callback(unit_state_cb)
        # Add the created ComputePilot to the UnitManager.
        #print "Registering Compute Pilot with Unit Manager ..."
        umgr.add_pilots(pilot)

        NUMBER_OF_ATOMS = total_file_lines
        WINDOW_SIZE =  win_size # i should define a "good" window based on division rules. in respect to the NUMBER_OF_ATOMS
        

        # submit CUs to pilot job
        cudesc_list = []
        sd_inter_in_list = list()
        for i in range(1,NUMBER_OF_ATOMS+1,WINDOW_SIZE):
            for j in range(i,NUMBER_OF_ATOMS,WINDOW_SIZE):
                    # I save here the results of each cu (matrix of all pairs) and stage it to create the graph
                INTERMEDIATE_FILE = "distances_%d_%d.npz.npy" % (i-1,j-1)
                sd_inter_out = {
                'source': INTERMEDIATE_FILE,
                # Note the triple slash, because of URL peculiarities
                'target': 'staging:///%s' % INTERMEDIATE_FILE,
                'action': rp.LINK
                }
                # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
                cudesc = rp.ComputeUnitDescription()
                cudesc.name='Euclidean_dist_%d_%d'%(i,j)
                cudesc.cores = 1
                cudesc.pre_exec=['module load python']
                cudesc.executable  = "python"
                cudesc.arguments   = ['atom_distances.py','trajectories.npy', WINDOW_SIZE, i, j,cutoff] # each CU should compute window size distances 
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
        #print "Submit Compute Units to Unit Manager ..."
        cu_set = umgr.submit_units (cudesc_list)
        cu_full_list.extend(cu_set)
        #print "Waiting for CUs to complete ..."
        umgr.wait_units()
        #print "All CUs completed successfully!"
    
        # Create and submit a task to the pilot. This CU is going to aggregate the results of  the previous tasks
        # create a big graph and find all the connected components of the graph
        sd_inter_in_list.append('find_connencted_components.py')
        # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
        cudesc = rp.ComputeUnitDescription()
        cudesc.name='ConnComp'
        cudesc.executable = "python"
        cudesc.cores = 1
        cudesc.pre_exec=['module load python']
        cudesc.arguments = ['find_connencted_components.py',NUMBER_OF_ATOMS,WINDOW_SIZE]
        cudesc.input_staging = sd_inter_in_list
        cudesc.output_staging = [{'source':'components.npz.npy',
                                  'target':'components.npz.npy',
                                  'action':rp.TRANSFER}]
        # -------- END USER DEFINED CU DESCRIPTION --------- #
        #print "Submit subgraph task"
        cuset = umgr.submit_units(cudesc)
        umgr.wait_units()
        #print "Task completed successfully!"

        cu_full_list.append(cuset)

        #print "Creating Profile"
        ProfFile = open('{0}.csv'.format(session_name),'w')
        ProfFile.write('CU,Name,StageIn,Allocate,Exec,StageOut,Done\n')
        for cu in cu_full_list:
            timing_str=[cu.uid,cu.name,'N/A','N/A','N/A','N/A','N/A','N/A']
            for states in cu.state_history:
                if states.as_dict()['state']=='AgentStagingInput':
                    timing_str[3]= (states.as_dict()['timestamp']-pilot.start_time).__str__()
                elif states.as_dict()['state']=='Allocating':
                    timing_str[4]= (states.as_dict()['timestamp']-pilot.start_time).__str__()
                elif states.as_dict()['state']=='Executing':
                    timing_str[5]= (states.as_dict()['timestamp']-pilot.start_time).__str__()
                elif states.as_dict()['state']=='AgentStagingOutput':
                    timing_str[6]= (states.as_dict()['timestamp']-pilot.start_time).__str__()
                elif states.as_dict()['state']=='Done':
                    timing_str[7]= (states.as_dict()['timestamp']-pilot.start_time).__str__()

            ProfFile.write(timing_str[0]+','+timing_str[1]+','+
                           timing_str[2]+','+timing_str[3]+','+
                           timing_str[4]+','+timing_str[5]+','+
                           timing_str[6]+','+timing_str[7]+'\n')
        ProfFile.close()


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
        #print "Closing session"
        session.close (cleanup=False)

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


#-------------------------------------------------------------------------------
