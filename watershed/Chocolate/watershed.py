import os
import sys
import time
import radical.pilot as rp

#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # Create a new session. No need to try/except this: if session creation fails, there is not much we can do anyways...
    #
    session = rp.Session()
    print "session id: %s" % session.uid

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally' clause...
    # 
    try:

        if len(sys.argv) == 5:

            pilot_cores = int(sys.argv[1])
            number_of_CUs = int(sys.argv[2])
            number_of_images = int(sys.argv[3])
            report_name = sys.argv[4]
            # FIXME:
            # fix localhost
            resource = "xsede.comet"
        
        elif len(sys.argv) == 6:
            
            pilot_cores = int(sys.argv[1])
            number_of_CUs = int(sys.argv[2])
            number_of_images = int(sys.argv[3])
            report_name = sys.argv[4]
            resource = sys.argv[5]

        else:
            
            print "Usage: python %s <pilot cores> <number of CUs> <number of images to segment> \
                            <name for the file to save the measurements> <resource> \
                                if resource == None, run it on localhost." % __file__ 
            
            print "Please run the script again!"
            sys.exit(-1)


        
        # Add a Pilot Manager
        print "Initiliazing Pilot Manager..."
        pmgr = rp.PilotManager(session=session)

        # Provide the Pilot Description
        #
        pdesc = rp.ComputePilotDescription()
        pdesc.resource  = resource
        #pdesc.project = "TG-MCB090174"      # for Stampede/Wrangler
        # pdesc.project   = "unc100"             # for Comet/Gordon
        pdesc.runtime   = 12                   # minutes
        pdesc.cores     = pilot_cores

        print "Submitting Compute Pilot to PilotManager"
        pilot = pmgr.submit_pilots(pdesc)


        
        print "Initiliazing Unit Manager"

        # Combine the ComputePilot, the ComputeUnits and a scheduler via a UnitManager object.
        umgr = rp.UnitManager(session=session)

        print "Registering Compute Pilot with Unit Manager"
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
                cudesc.arguments = ['watershed_lines.py', step, step+images_in_each_CU-1]
                step += images_in_each_CU
            else:
                cudesc.arguments = ['watershed_lines.py', step, step+images_in_each_CU]
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
        print 'Submitting the CUs to the Unit Manager...'
        cu_set = umgr.submit_units(cudesc_list)        
        cu_list.extend(cu_set)

        # wait for all units to finish
        states = umgr.wait_units()      

    except Exception as e:
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        
        # the callback called sys.exit(), and we can here catch the corresponding 
        # KeyboardInterrupt exception for shutdown.  We also catch SystemExit 
        # (which gets raised if the main threads exits for some other reason).
        
        print "need to exit now: %s" % e

    finally:
        
        # always clean up the session, no matter if we caught an exception or not.
        #
        
        print "closing session"
        session.close ()

        # the above is equivalent to session.close (cleanup=True, terminate=True)
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in example).

#-------------------------------------------------------------------------------
