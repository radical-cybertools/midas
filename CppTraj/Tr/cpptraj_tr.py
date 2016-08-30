#!/usr/bin/env python

__copyright__ = "Copyright 2015-2016, http://radical.rutgers.edu"
__license__   = "MIT"

import os

os.environ['RADICAL_PILOT_VERBOSE'] = 'DEBUG'

import sys
import radical.pilot as rp

MY_STAGING_AREA = 'staging:///'

def create_cpptrajin(frames,cores):
    if (frames%cores)!=0:
        step = frames/cores + 1
    else:
        step = frames/cores
    for i in range(1,cores+1):
        filename = open('cpptraj%d.in'%i,'w')
        filename.write('trajin GAAC3-strip.crd %d %d\n'%((i-1)*step + 1, i*step if (i*step)<frames else frames))
        filename.write('rmsd first @H* rsmd_exp%d.dat\n'%i)
        filename.write('run\n')
        filename.write('exit\n')
        filename.close()



if __name__ == "__main__":

    # Read the number of the divisions you want to create
    args = sys.argv[1:]
    if len(args) < 2:
        print "Usage: "
        print "python cpptraj_tr.py <cores> <report_name>"
        sys.exit(-1)
    cores = int(sys.argv[1]) # number of cores
    report_name = sys.argv[2]

    total_frames = 881372

    create_cpptrajin(total_frames,cores)

    try:
        # Create a new session.
        session = rp.Session()

        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # 
        pdesc = rp.ComputePilotDescription ()
        pdesc.resource = "xsede.stampede" # NOTE: This is a "label", not a hostname
        pdesc.runtime  = 40 # minutes
        pdesc.cores    = cores
        pdesc.cleanup  = False
        pdesc.queue    = 'development'
        pdesc.project  = 'TG-MCB090174'

        # submit the pilot.
        pilot = pmgr.submit_pilots(pdesc)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager (session=session,
                               scheduler=rp.SCHED_DIRECT)

        # Add the created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)       

        # submit CUs to pilot job
        cudesc_list = []
        for i in range(1,cores+1):
            cudesc = rp.ComputeUnitDescription()
            cudesc.pre_exec=['module load netcdf pnetcdf']
            cudesc.name = 'cpptraj_tr%d'%i
            #cudesc.executable='env'
            #cudesc.arguments=''
            cudesc.executable  = "/home1/00301/tg455746/GitHub/cpptraj/bin/cpptraj"
            cudesc.arguments   = ['-p','GAAC-ions.topo','-i', 'cpptraj.in']
            cudesc.input_staging = [{'source': '/work/03170/tg824689/cpptraj_exp/rmsd_tr/GAAC3-strip.crd', 
                                     'target': 'GAAC3-strip.crd',
                                     'action': rp.LINK
                                    },
                                    {'source': '/work/03170/tg824689/cpptraj_exp/rmsd_tr/GAAC-ions.topo', 
                                     'target': 'GAAC-ions.topo',
                                     'action': rp.LINK
                                    },
                                    {'source': 'cpptraj%d.in'%i, 
                                     'target': 'cpptraj.in',
                                     'action': rp.TRANSFER
                                    }]
            # -------- END USER DEFINED CU DESCRIPTION --------- #
            cudesc_list.append(cudesc)

        # Submit the previously created ComputeUnit descriptions to the
        cu_set = umgr.submit_units (cudesc_list)
        umgr.wait_units()

        ProfFile = open('{1}-{0}.csv'.format(cores,report_name),'w')
        ProfFile.write('CU,Name,StageIn,Allocate,Exec,StageOut,Done\n')
        for cu in cu_set:
            timing_str=[cu.uid,cu.name,'N/A','N/A','N/A','N/A','N/A']
            for states in cu.state_history:
                if states.as_dict()['state']=='AgentStagingInput':
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

    finally:
        # always clean up the session, no matter if we caught an exception or
        # not.
        session.close (cleanup=False)

        # the above is equivalent to
        #
        #   session.close (cleanup=True, terminate=True)
        #
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in our example).


#-------------------------------------------------------------------------------
