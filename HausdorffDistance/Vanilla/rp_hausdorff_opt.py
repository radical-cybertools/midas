import os
import sys
import saga
import radical.pilot as rp
import numpy as np
import ast
from datetime import datetime
from datetime import timedelta

def pilot_state_cb (pilot, state) :
    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)
    if  state == rp.FAILED :
        sys.exit (1)

def unit_state_cb (unit, state) :
    print "[Callback]: ComputeUnit  '%s' state: %s." % (unit.uid, state)

def all_pairs (n) :
    ret = list ()
    for i in range (1, n+1) :
        for j in range (i+1, n+1) :
            ret.append ([i, j])
    return ret

#----------------------------------------------------------------------------
if __name__ == "__main__":
    print datetime.now()
    NUMBER_OF_TRAJECTORIES = 192
    ATOM_SEL = 'ca'
    TRAJ_SIZE='short'
    MY_STAGING_AREA = 'staging:///'
    TRJ_LOCATION = '' #Path that points to the folder the trajectories are
    SHARED_HAUSDORFF = 'hausdorff_opt.py'
    WINDOW_SIZE = 24

    try:
        session   = rp.Session ()
        c         = rp.Context ('ssh')
        c.user_id = ''
        session.add_context (c)

        print "initialize pilot manager ..."
        pmgr = rp.PilotManager (session=session)
        pmgr.register_callback (pilot_state_cb)

        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = "xsede.stampede"
        pdesc.runtime  = 30 # minutes
        pdesc.cores    = 32
        pdesc.project  = "" #Project allocation
        pdesc.cleanup  = False

        # submit the pilot.
        print "submit pilot ..."
        pilot = pmgr.submit_pilots (pdesc)

        print "initialize unit manager ..."
        umgr = rp.UnitManager  (session=session, scheduler=rp.SCHED_BACKFILLING)
        umgr.register_callback (unit_state_cb)

        print "add pilot to unit manager ..."
        umgr.add_pilots(pilot)

        print "stage shared data ..."
        fshared_list   = list()
        
              
        fname_stage = []
        # stage all files to the staging area
        src_url = 'file://%s/%s' % (os.getcwd(), SHARED_HAUSDORFF)

        print src_url

        sd_pilot = {'source': src_url,
                    'target': os.path.join(MY_STAGING_AREA, SHARED_HAUSDORFF),
                    'action': rp.TRANSFER,
        }
        fname_stage.append (sd_pilot)
           
        # Synchronously stage the data to the pilot
        pilot.stage_in(fname_stage)

        print "describe compute units ..."

        print "all pairs: "
        # we create one CU for each unique pair of trajectories
        cudesc_list = []

        pairs = all_pairs (NUMBER_OF_TRAJECTORIES)

        
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
                #cudesc.pre_exec      = ["module load python/2.7.3-epd-7.3.2"] #Only for Stampede
                cudesc.input_staging = fshared
                cudesc.arguments     = ['hausdorff_opt.py', range(i,i+WINDOW_SIZE), range(j,j+WINDOW_SIZE), ATOM_SEL, TRAJ_SIZE]
                cudesc.cores         = 1

                cudesc_list.append (cudesc)

                print "Computing All-Pair for ", range(i,i+WINDOW_SIZE), "and ",range(j,j+WINDOW_SIZE)

        # submit, run and wait and...
        print "submit units to unit manager ..."
        units = umgr.submit_units (cudesc_list)

        print "wait for units ..."
        starting_time = datetime.now()
        umgr.wait_units()
        ending_time = datetime.now()

        units_total_time = timedelta(0)

        fp=open('output_{0}_{2}_{3}_traj_stampede_{1}cores.log'.format(NUMBER_OF_TRAJECTORIES,pdesc.cores,ATOM_SEL,TRAJ_SIZE),'w')
        comp_starting_time = list()
        comp_ending_time = list()
        dists = np.zeros((NUMBER_OF_TRAJECTORIES,NUMBER_OF_TRAJECTORIES))
        #... voila!
        for unit in units :
            lines=unit.stdout.splitlines()
            for line in lines:
                parts = line.split(':')
                pos = ast.literal_eval(parts[0])
                dists[pos[0]-1][pos[1]-1]=float(parts[1])
                dists[pos[1]-1][pos[0]-1]=float(parts[1])
            print "* Unit %s @ %s \n"      \
                  "\t   state    : %s\n"   \
                  "\t   exit code: %s\n"   \
                  "\t   started  : %s\n"   \
                  "\t   finished : %s\n"   \
                  "\t   output   : %s\n"   \
                  "\t   error    : %s\n\n" \
                % (unit.uid,       unit.execution_locations, unit.state, 
                   unit.exit_code, unit.start_time,          unit.stop_time,
                   unit.stdout,    unit.stderr)
            comp_starting_time.append(unit.start_time)
            comp_ending_time.append(unit.stop_time)
            fp.write("* Unit %s @ %s \n"   \
                  "\t   state    : %s\n"   \
                  "\t   exit code: %s\n"   \
                  "\t   started  : %s\n"   \
                  "\t   finished : %s\n"   \
                  "\t   output   : %s\n"   \
                  "\t   error    : %s\n\n" \
                % (unit.uid,       unit.execution_locations, unit.state, 
                   unit.exit_code, unit.start_time,          unit.stop_time,
                   unit.stdout,    unit.stderr))
            units_total_time = units_total_time + unit.stop_time - unit.start_time
        np.save('starting_times_{0}_{2}_{3}_traj_stampede_{1}cores.npz.npy'.format(NUMBER_OF_TRAJECTORIES,pdesc.cores,ATOM_SEL,TRAJ_SIZE),np.array(comp_starting_time))
        np.save('ending_times_{0}_{2}_{3}_traj_stampede_{1}cores.npz.npy'.format(NUMBER_OF_TRAJECTORIES,pdesc.cores,ATOM_SEL,TRAJ_SIZE),np.array(comp_ending_time))
        np.save('rp_hausdorff_distances.npz.npy',dists)
        comp_ending_time.sort()
        comp_starting_time.sort()
        print "Time To Completion with queue time (in secs) is ",(ending_time-starting_time).total_seconds()
        print "Total Time to Completion (in secs) is ",(comp_ending_time[-1]-comp_starting_time[0]).total_seconds()
        print "Summation of Units Execution Time (in secs) is ", units_total_time.total_seconds()
        fp.write("Time To Completion with queue time (in secs) is  %f\n"%(ending_time-starting_time).total_seconds())
        fp.write("Total Time to Completion (in secs) is %f\n"%(comp_ending_time[-1]-comp_starting_time[0]).total_seconds())
        fp.write("Summation of Units Execution Time (in secs) is %f\n"%units_total_time.total_seconds())
        fp.write("First CU started @ %s\n"%comp_starting_time[0])
        fp.write("Last CU ended @ %s\n"%comp_ending_time[-1])
        fp.close()
    except Exception as e:
        import traceback
        traceback.print_exc ()
        print "An error occurred: %s" % ((str(e)))
        sys.exit (-1)

    except KeyboardInterrupt :
        print "Execution was interrupted"
        sys.exit (-1)

    finally :
        print "Closing session, exiting now ..."
        session.close()
        print datetime.now()

