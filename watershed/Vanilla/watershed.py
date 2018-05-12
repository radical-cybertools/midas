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

    parser = argparse.ArgumentParser()

    # pilot cores
    parser.add_argument('cores',
                        type=int,
                        help='number of cores to use')
    # num compute units
    parser.add_argument('cu',
                        type=int,
                        help='number of CUs to submit')
    # number images
    parser.add_argument('images',
                        type=int,
                        help='number of images to analyze')
    # project
    parser.add_argument('project',
                        type=str,
                        help='project to obtain allocations from')
    # resource
    parser.add_argument('resource',
                        type=str,
                        help='resource to use')
    # queue
    parser.add_argument('queue',
                        type=str,
                        help='queue to use')
    # data path to input and outputs folders
    parser.add_argument('path',
                        type=str,
                        help='path of data input and outputs folders')
    # image extension
    parser.add_argument('-e', '--imgext',
                        type=str,
                        default='.jpg',
                        help='extension of image files being read in (defaults to .jpg)')
    # walltime
    parser.add_argument('-w', '--walltime',
                        type=int,
                        default=15,
                        help='specify the walltime in minutes (defaults to 15)')
    # brightness background
    parser.add_argument('-b', '--brightness',
                        type=int,
                        default=0, 
                        choices=[0, 1],
                        help='set image background brightness (defaults to 0)')
    # report name
    parser.add_argument('-r', '--report',
                        type=str,       
                        default=None,
                        help='report name used as name of session folder (defaults to "watershed_report")')
    # outputs folder name
    parser.add_argument('-i', '--inputs',
                        type=str,       
                        default='inputs',
                        help='inputs folder name (defaults to "inputs")')
    # outputs folder name
    parser.add_argument('-o', '--outputs',
                        type=str,       
                        default='outputs',
                        help='outputs folder name (defaults to "outputs")')
    # verbosity
    parser.add_argument('-v', '--verbosity',
                        action='count', 
                        default=2,
                        help='increase outputs verbosity (defaults to 2)')

    # retrieve arguments
    args = parser.parse_args()

    # set variables
    pilot_cores         = args.cores
    number_of_CUs       = args.cu
    number_of_images    = args.images
    project             = args.project
    resource            = args.resource
    queue               = args.queue
    walltime            = args.walltime
    bright_background   = args.brightness
    report              = args.report
    path                = args.path
    inputs              = args.inputs
    outputs             = args.outputs
    verbosity           = args.verbosity
    if args.imgext[0] == '.': 
        imgext = args.imgext
    else :               
        imgext = '.' + args.imgext

    # FIXME: quick fix to bypass Saga Layer Error when project not None and resource not local
    if 'local' in resource:
        project = None
        queue   = None

    if verbosity >= 2:
        print('Input Arguments:')
        pp([   ['pilot_cores      ' , pilot_cores       ],
               ['number_of_CUs    ' , number_of_CUs     ],
               ['number_of_images ' , number_of_images  ],
               ['project          ' , project           ],
               ['resource         ' , resource          ],
               ['queue            ' , queue             ],
               ['walltime         ' , walltime          ],
               ['bright_background' , bright_background ],
               ['report           ' , report            ],
               ['path             ' , path              ],
               ['inputs           ' , inputs            ],
               ['outputs          ' , outputs           ]
           ])
    if verbosity >= 1:
        print 'Arguments are valid'

    # Create a new session. No need to try/except this: if session creation fails, there is not much we can do anyways...
    #
    session = rp.Session(uid=report)

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally' clause...
    # 
    try:
        
        # Add a Pilot Manager
        pmgr = rp.PilotManager(session=session)

        # Provide the Pilot Description
        #
        pdesc           = rp.ComputePilotDescription()
        pdesc.resource  = resource
        pdesc.project   = project
        pdesc.runtime   = walltime
        pdesc.cores     = pilot_cores
        pdesc.queue     = queue
        pdesc.cleanup   = False

        pilot = pmgr.submit_pilots(pdesc)

        # Combine the ComputePilot, the ComputeUnits and a scheduler via a UnitManager object.
        umgr = rp.UnitManager(session=session)

        umgr.add_pilots(pilot) 
        
        images_in_each_CU = number_of_images / number_of_CUs
        additional_load   = number_of_images % number_of_CUs

        step = 0
        cu_list     = list()
        cudesc_list = list()    
        for i in xrange(number_of_CUs):

            cudesc = rp.ComputeUnitDescription()

            cudesc.executable  = 'python'
            
            if (additional_load == 0):
                cudesc.arguments = ['watershed_lines.py', 
                                    path, 
                                    step, 
                                    step+images_in_each_CU-1, 
                                    bright_background,
                                    imgext,
                                    inputs,
                                    outputs]
                step += images_in_each_CU
            else:
                cudesc.arguments = ['watershed_lines.py', 
                                    path,
                                    step, 
                                    step+images_in_each_CU, 
                                    bright_background,
                                    imgext,
                                    inputs,
                                    outputs]
                step += images_in_each_CU + 1
                additional_load -= 1

            staging_directive = {
                'source'  : 'client://watershed_lines.py',
                'target'  : 'unit://watershed_lines.py',
                'action'  : rp.TRANSFER,
                # 'flags'   : None,
                # 'priority': 0
            }

            cudesc.input_staging = [staging_directive]

            cudesc_list.append(cudesc)
            
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
        
        session.close(cleanup=False)

        # the above is equivalent to session.close (cleanup=True, terminate=True)
        # it will thus both clean out the session's database record, and kill
        # all remaining pilots (none in example).

#-------------------------------------------------------------------------------
