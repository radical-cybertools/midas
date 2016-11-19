import os
import time
os.environ['RADICAL_PILOT_VERBOSE']='DEBUG'

import sys
import radical.pilot as rp

#----------------------------------------------------------------------------
if __name__ == "__main__":

    NUMBER_OF_TRAJECTORIES = 
    ATOM_SEL = 
    TRAJ_SIZE=
    MY_STAGING_AREA = 'staging:///'
    TRJ_LOCATION =  #Path that points to the folder the trajectories are
    SHARED_HAUSDORFF = 'hausdorff_opt.py'
    WINDOW_SIZE = int(sys.argv[1])
    cores = int(sys.argv[2])
    session_name = sys.argv[3]

    try:
        session   = rp.Session (name=session_name)
        c         = rp.Context ('ssh')
        session.add_context (c)

        print "initialize pilot manager ..."
        pmgr = rp.PilotManager (session=session)
        #pmgr.register_callback (pilot_state_cb)

        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = ""
        pdesc.runtime  = 20 # minutes
        pdesc.cores    = cores
        pdesc.project  = "" #Project allocation
        pdesc.cleanup  = False

        # submit the pilot.
        pilot = pmgr.submit_pilots (pdesc)

        #initialize unit manager
        umgr = rp.UnitManager  (session=session, scheduler=rp.SCHED_DIRECT_SUBMISSION)

        #add pilot to unit manager
        umgr.add_pilots(pilot)

        fshared_list   = list()
        fname_stage = []
        # stage all files to the staging area
        src_url = 'file://%s/%s' % (os.getcwd(), SHARED_HAUSDORFF)

        #print src_url

        sd_pilot = {'source': src_url,
                    'target': os.path.join(MY_STAGING_AREA, SHARED_HAUSDORFF),
                    'action': rp.TRANSFER,
        }
        fname_stage.append (sd_pilot)
           
        # Synchronously stage the data to the pilot
        pilot.stage_in(fname_stage)

        # we create one CU for each unique pair of trajectories
        cudesc_list = []

        for i in range(1,NUMBER_OF_TRAJECTORIES+1,WINDOW_SIZE):
            for j in range(i,NUMBER_OF_TRAJECTORIES+1,WINDOW_SIZE):
                fshared = list()
                shared = {'source': os.path.join(MY_STAGING_AREA, SHARED_HAUSDORFF),
                         'target': SHARED_HAUSDORFF,
                         'action': rp.LINK}
                fshared.append(shared)

                if i == j:
                    shared = [{'source': 'file://%s/trj_%s_%03i.npz.npy' % (TRJ_LOCATION,ATOM_SEL,k),
                              'target' : 'trj_%s_%03i.npz.npy' % (ATOM_SEL,k),
                              'action' : rp.LINK} for k in range(i,i+WINDOW_SIZE)]
                    fshared.extend(shared)
                else:
                    shared = [{'source': 'file://%s/trj_%s_%03i.npz.npy' % (TRJ_LOCATION,ATOM_SEL,k),
                              'target' : 'trj_%s_%03i.npz.npy' % (ATOM_SEL,k),
                              'action' : rp.LINK} for k in range(i,i+WINDOW_SIZE)]
                    fshared.extend(shared)
                    shared = [{'source': 'file://%s/trj_%s_%03i.npz.npy' % (TRJ_LOCATION,ATOM_SEL,k),
                              'target' : 'trj_%s_%03i.npz.npy' % (ATOM_SEL,k),
                              'action' : rp.LINK} for k in range(j,j+WINDOW_SIZE)]
                    fshared.extend(shared)

            # define the compute unit, to compute over the trajectory pair
                cudesc = rp.ComputeUnitDescription()
                cudesc.executable    = "python"
                cudesc.pre_exec      = ["module load python"] #Only for Stampede
                cudesc.input_staging = fshared
                cudesc.arguments     = ['hausdorff_opt.py', range(i,i+WINDOW_SIZE), range(j,j+WINDOW_SIZE), ATOM_SEL, TRAJ_SIZE]
                cudesc.cores         = 1

                cudesc_list.append (cudesc)

        # submit, run and wait and...
        #print "submit units to unit manager ..."
        units = umgr.submit_units (cudesc_list)

        #print "wait for units ..."
        umgr.wait_units()

        print "Creating Profile"
        ProfFile = open('{0}.csv'.format(session.name),'w')
        ProfFile.write('CU,New,StageIn,Allocate,Exec,StageOut,Done\n')
        for cu in units:
            timing_str=[cu.uid,'N/A','N/A','N/A','N/A','N/A','N/A']
            for states in cu.state_history:
                if states.as_dict()['state']=='Scheduling':
                    timing_str[1]= (states.as_dict()['timestamp']-pilot.start_time).__str__()
                elif states.as_dict()['state']=='AgentStagingInput':
                    timing_str[2]= (states.as_dict()['timestamp']-pilot.start_time).__str__()
                elif states.as_dict()['state']=='Allocating':
                    timing_str[3]= (states.as_dict()['timestamp']-pilot.start_time).__str__()
                elif states.as_dict()['state']=='Executing':
                    timing_str[4]= (states.as_dict()['timestamp']-pilot.start_time).__str__()
                elif states.as_dict()['state']=='AgentStagingOutput':
                    timing_str[5]= (states.as_dict()['timestamp']-pilot.start_time).__str__()
                elif states.as_dict()['state']=='Done':
                    timing_str[6]= (states.as_dict()['timestamp']-pilot.start_time).__str__()

            ProfFile.write(timing_str[0]+','+timing_str[1]+','+
                           timing_str[2]+','+timing_str[3]+','+
                           timing_str[4]+','+timing_str[5]+','+
                           timing_str[6]+'\n')
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

    finally :
        print "Closing session, exiting now ..."
        session.close(cleanup=False)

