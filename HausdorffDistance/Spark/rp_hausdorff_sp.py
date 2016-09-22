import os

os.environ['RADICAL_PILOT_VERBOSE']='DEBUG'

import sys
import radical.pilot as rp

#----------------------------------------------------------------------------
if __name__ == "__main__":
    
    args = sys.argv[1:]
    if len(args) < 5:
        print "Usage: "
        print "python rp_hausdorff_sp.py  <cores> <NumOfTrj> <Sel> <Size> <WindowSize> <TrjLocation>"
        sys.exit(-1)
    
    cores = int(sys.argv[1]) # number of cores
    NumOfTrj = int(sys.argv[2])
    Sel = sys.argv[3]
    Size = sys.argv[4]
    WindowSize = int(sys.argv[5])
    TrjLocation = sys.argv[6] #Path that points to the folder the trajectories are

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
        pdesc.runtime  = 60 # minutes
        pdesc.cores    = cores
        pdesc.project  = "" #Project allocation
        pdesc.cleanup  = False

        # submit the pilot.
        pilot = pmgr.submit_pilots (pdesc)

        umgr = rp.UnitManager  (session=session, scheduler=rp.SCHED_DIRECT_SUBMISSION)
        umgr.add_pilots(pilot)

       
        InputFiles = [{'source': 'file://%s/trj_%s_%03i.npz.npy' % (TrjLocation,Sel,k),
                   'target' : 'trj_%s_%03i.npz.npy' % (Sel,k),
                   'action' : rp.LINK} for k in range(1,1+NumOfTrj)]

        InputFiles.append('hausdorff.py')
        # define the compute unit, to compute over the trajectory pair
        cudesc = rp.ComputeUnitDescription()
        cudesc.executable    = "spark-submit"
        cudesc.input_staging = InputFiles
        cudesc.arguments     = ['--conf', 'spark.driver.maxResultSize=5g', \
                                '--executor-memory 60g --driver-memory 30g',\
                                'hausdorff.py %d %s %s %d' % (NumOfTrj,Sel,Size,WindowSize)]
        cudesc.cores         = cores

        # submit, run and wait and...
        units = umgr.submit_units (cudesc)

        umgr.wait_units()
    except Exception as e:
        import traceback
        traceback.print_exc ()
        print "An error occurred: %s" % ((str(e)))
        sys.exit (-1)

    except KeyboardInterrupt :
        print "Execution was interrupted"
        sys.exit (-1)

    finally :
        session.close(cleanup=False)

