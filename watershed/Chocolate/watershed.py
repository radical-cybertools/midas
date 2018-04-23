import os
import sys
import time
import argparse
import radical.pilot as rp

import pprint
pp = pprint.PrettyPrinter().pprint

#------------------------------------------------------------------------------
#
if __name__ == '__main__':

    '''
    Add arguments through argparse
    The following arguments are required:
    - pilot cores
    - number compute units
    - number images

    The following arguments will default if no args given:
    - report name (default: watershed_report)
    - resource    (default: local.localhost_anaconda)
    '''

    parser = argparse.ArgumentParser()

    # pilot cores
    parser.add_argument('cores',                type=int,
                        help='pilot cores: specify the number of cores to use')
    # num compute units
    parser.add_argument('cu',                   type=int,
                        help='compute units: specify the number of CUs to use')
    # pilot cores
    parser.add_argument('images',               type=int,
                        help='specify the number of images to analyze')
    # brightness
    parser.add_argument('-b','--brightness',    type=int, default=0, choices=[0, 1],
                        help='set image background brightness')
    # report name
    parser.add_argument('-r','--report',        type=str, default='watershed_report',
                        help='report name')
    # resource
    parser.add_argument('-R','--resource',      type=str, default='local.localhost_anaconda',
                        help='specify the resource to use')
    # verbosity
    parser.add_argument('-v', '--verbosity',    action='count', default=0,
                        help='increase output verbosity')

    # retrieve arguments
    args = parser.parse_args()

    # set variables
    pilot_cores         = args.cores
    number_of_CUs       = args.cu
    number_of_images    = args.images
    report_name         = args.report
    resource            = args.resource
    bright_background   = args.brightness          

    if args.verbosity >= 2:
        print('Input Arguments')
        pp([   ['pilot_cores      ' , pilot_cores],
               ['number_of_CUs    ' , number_of_CUs],
               ['number_of_images ' , number_of_images],
               ['report_name      ' , report_name],
               ['resource         ' , resource],
               ['bright_background' , bright_background]
           ])
    if args.verbosity >= 1:
        print 'Arguments are valid'

    # Create a new session. No need to try/except this: if session creation fails, there is not much we can do anyways...
    #
    session = rp.Session()

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally' clause...
    # 
    try:
        
        # Add a Pilot Manager
        pmgr = rp.PilotManager(session=session)

        # Provide the Pilot Description
        #
        pdesc = rp.ComputePilotDescription()
        pdesc.resource  = resource
        #pdesc.project = 'TG-MCB090174'      # for Stampede/Wrangler
        # pdesc.project   = 'unc100'             # for Comet/Gordon
        pdesc.runtime   = 12                   # minutes
        pdesc.cores     = pilot_cores

        pilot = pmgr.submit_pilots(pdesc)



        # Combine the ComputePilot, the ComputeUnits and a scheduler via a UnitManager object.
        umgr = rp.UnitManager(session=session)

        umgr.add_pilots(pilot) 
        
        
        images_in_each_CU = number_of_images / number_of_CUs
        additional_load = number_of_images % number_of_CUs

        step = 0
        cu_list     = list()
        cudesc_list = list()    
        for i in xrange(number_of_CUs):

            cudesc = rp.ComputeUnitDescription()
            
            # $USER = statho
            # FIXME
            # appropriate arguments?
            # cudesc.pre_exec = ['module load python', 'module load scipy', '. /oasis/scratch/comet/$USER/temp_project/ve/bin/activate']
            # cudesc.pre_exec = ['module load python', 'module load scipy']
            cudesc.executable  = 'python'
            
            if (additional_load == 0):
                cudesc.arguments = ['watershed_lines.py', step, step+images_in_each_CU-1, bright_background]
                step += images_in_each_CU
            else:
                cudesc.arguments = ['watershed_lines.py', step, step+images_in_each_CU  , bright_background]
                step += images_in_each_CU + 1
                additional_load -= 1

            cudesc.input_staging = ['watershed_lines.py']

            # staging_directive = {
            #     'source'  : 'client://watershed_lines.py', # see 'Location' below
            #     'target'  : 'unit://watershed_lines.py', # see 'Location' below
            #     'action'  : rp.TRANSFER, # See 'Action operators' below
            #     'flags'   : None, # See 'Flags' below
            #     'priority': 0     # Control ordering of actions (unused)

            # }

            #cudesc.input_staging = [staging_directive]

            cudesc_list.append(cudesc)
            
            
        # FIXME
        # cu_list is empty already
        # why not do this -> cu_list = umgr.submit_units(cudesc_list)
        cu_set = umgr.submit_units(cudesc_list)        
        cu_list.extend(cu_set)

        # wait for all units to finish
        states = umgr.wait_units()      

    except Exception as e:
        # Something unexpected happened in the pilot code above
        print 'caught Exception: %s' % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        
        # the callback called sys.exit(), and we can here catch the corresponding 
        # KeyboardInterrupt exception for shutdown.  We also catch SystemExit 
        # (which gets raised if the main threads exits for some other reason).
        
        print 'need to exit now: %s' % e

    finally:
        
        # always clean up the session, no matter if we caught an exception or not.
        #
        
        session.close ()

        # the above is equivalent to session.close (cleanup=True, terminate=True)
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in example).

#-------------------------------------------------------------------------------
