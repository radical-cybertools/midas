import os
import sys
import time
import radical.pilot as rp


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

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if state == rp.FAILED:
        print "stderr: %s" % unit.stderr
        # do not exit



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

        start_time = time.time()

        if len(sys.argv) == 5:

            pilot_cores = int(sys.argv[1])
            number_of_CUs = int(sys.argv[2])
            number_of_images = int(sys.argv[3])
            report_name = sys.argv[4]
            resource = "localhost"
        
        elif len(sys.argv) == 6:
            
            pilot_cores = int(sys.argv[1])
            number_of_CUs = int(sys.argv[2])
            number_of_images = int(sys.argv[3])
            report_name = sys.argv[4]
            resource = sys.argv[5]
    
            c = rp.Context('ssh')
            #c.user_id = "tg835489"            # for Stampede/Wrangler
            c.user_id = "statho"               # for Comet/Gordon
            session.add_context(c)
            #path = '/oasis/scratch/comet/$USER/temp_project/Dataset_16GB/inputs/'

        else:
            
            print "Usage: python %s <pilot cores> <number of CUs> <number of images to segment> \
                            <name for the file to save the measurements> <resource> \
                                if resource == None, run it on localhost." % __file__ 
            
            print "Please run the script again!"
            sys.exit(-1)


        
        # Add a Pilot Manager
        print "Initiliazing Pilot Manager..."
        pmgr = rp.PilotManager(session=session)

        # Register our callback with our Pilot Manager. This callback will get
        # called every time the pilot managed by the PilotManager changes its state
        #
        pmgr.register_callback(pilot_state_cb)

        # Provide the Pilot Description
        #
        pdesc = rp.ComputePilotDescription()
        pdesc.resource = resource
        #pdesc.project = "TG-MCB090174"      # for Stampede/Wrangler
        pdesc.project = "unc100"             # for Comet/Gordon
        pdesc.runtime = 12                   # minutes
        pdesc.cores = pilot_cores

        print "Submitting Compute Pilot to PilotManager"
        pilot = pmgr.submit_pilots(pdesc)


        
        print "Initiliazing Unit Manager"

        # Combine the ComputePilot, the ComputeUnits and a scheduler via a UnitManager object.
        umgr = rp.UnitManager(session=session, scheduler=rp.SCHED_DIRECT_SUBMISSION)

        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager change their state.
        #
        print 'Registering the callbacks so we can keep an eye on the CUs'
        umgr.register_callback(unit_state_cb)

        print "Registering Compute Pilot with Unit Manager"
        umgr.add_pilots(pilot) 
        
        
        images_in_each_CU = number_of_images / number_of_CUs
        additional_load = number_of_images % number_of_CUs

        step = 0
        cu_list = []
        cudesc_list = []
        
        for i in xrange(number_of_CUs):

            cudesc = rp.ComputeUnitDescription()
            
            # $USER = statho
            cudesc.pre_exec = ['module load python', 'module load scipy', '. /oasis/scratch/comet/$USER/temp_project/ve/bin/activate']    
            cudesc.executable  = 'python'
            
            if (additional_load == 0):
                
                cudesc.arguments = ['watershed_lines.py', step, step+images_in_each_CU-1]
                step += images_in_each_CU

            else:

                cudesc.arguments = ['watershed_lines.py', step, step+images_in_each_CU]
                step += images_in_each_CU + 1
                additional_load -= 1

            cudesc.input_staging = ['watershed_lines.py']
            cudesc_list.append(cudesc)
            
            

        print 'Submitting the CUs to the Unit Manager...'
        cu_set = umgr.submit_units(cudesc_list)        
        cu_list.extend(cu_set)

        # wait for all units to finish
        umgr.wait_units()


        print "Creating Profile"
        profiling = open('{}.csv'.format(report_name), 'w')
        profiling.write('CU, New, StageIn, Allocate, Exec, StageOut, Done\n')
        
        for cu in cu_list:
            
            cu_info = [cu.uid, '', '', '', '', '', '']
            
            for states in cu.state_history:

                if states.as_dict()['state'] == 'Scheduling':
                    cu_info[1] = str((states.as_dict()['timestamp'] - pilot.start_time))
                
                elif states.as_dict()['state'] == 'AgentStagingInput':
                    cu_info[2] = str((states.as_dict()['timestamp'] - pilot.start_time))
                
                elif states.as_dict()['state'] == 'Allocating':
                    cu_info[3] = str((states.as_dict()['timestamp'] - pilot.start_time))
                
                elif states.as_dict()['state'] == 'Executing':
                    cu_info[4] = str((states.as_dict()['timestamp'] - pilot.start_time))
                
                elif states.as_dict()['state'] == 'AgentStagingOutput':
                    cu_info[5] = str((states.as_dict()['timestamp'] - pilot.start_time))
                
                elif states.as_dict()['state'] == 'Done':
                    cu_info[6] = str((states.as_dict()['timestamp'] - pilot.start_time))

            profiling.write(cu_info[0] + ',' + cu_info[1] + ',' + cu_info[2] + ',' + cu_info[3] 
                                + ',' + cu_info[4] + ',' + cu_info[5] + ',' + cu_info[6] + '\n')

        finish_time = time.time()
        total_time = (finish_time - start_time) / 60.0  # total execution time
        profiling.write("\nTTC," + str(total_time))

        profiling.close()
        
        print 'The total execution time is: %f minutes' % total_time
        

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
