__author__ = "George Chantzialexiou"
__copyright__ = "Copyright 2013-2014, http://radical.rutgers.edu"
__license__ = "MIT"

import sys
import os
import radical.pilot as rp
import copy
import numpy as np
import mmap
import argparse

#SHARED_INPUT_FILE = 'dataset.in'
MY_STAGING_AREA = 'staging:///'

""" DESCRIPTION:  k-means
For every task A_n (mapper)  is started
"""

# ------------------------------------------------------------------------------
#
def get_distance(dataPoint, centroid):
    # Calculate Euclidean distance.
    return np.sqrt(sum((dataPoint - centroid) ** 2))
# ------------------------------------------------------------------------------
#   
if __name__ == "__main__":

    # Read the number of the divisions you want to create
    parser = argparse.ArgumentParser()
    parser.add_argument("k", help="Number of Clusters")
    parser.add_argument("dim", help="The data dimensionality")
    parser.add_argument("tasks", help="Total Number of tasks")
    parser.add_argument("cores",help="Number of cores to be used")
    parser.add_argument("input_file",help="Name of the input file")
    parser.add_argument("queue", help='Queue to submit the execution')
    parser.add_argument("resource", help='Resource label')
    parser.add_argument("walltime", help='Walltime')
    parser.add_argument("project", help='Project ID')
    args = parser.parse_args()
    k = int(args.k)  # number of the divisions - clusters
    dim = int(args.dim)
    tasks = int(args.tasks)
    CPUs = int(args.cores) # number of cores
    SHARED_INPUT_FILE = args.input_file
    DIMENSIONS = dim

    print 'Clusters: {0}, Dimensions: {1}, Tasks: {2}, Cores: {3}, Input File: {4}'.format(k,dim,tasks,CPUs,SHARED_INPUT_FILE)
    # Check if the dataset exists  and count the total number of lines of the dataset
    try:
    	data = open(SHARED_INPUT_FILE,'r')
    except IOError:
    	print "Missing data-set. file! Check the name of the dataset"
    	sys.exit(-1)
    total_file_lines =  sum(1 for _ in data)

    if (total_file_lines % DIMENSIONS)!= 0:
        print " Wrong input! Dataset is not %d diamensional." % DIMENSIONS
        sys.exit(-1) 

	#-----------------------------------------------------------------------
    #Choose randomly k elements from the dataset as centroids
    data.seek(0,0) # move fd to the beginning of the file
    centroid = list()
    for i in range(0,DIMENSIONS*k):
        centroid.append(data.readline())
    data.close()
    centroid =  map(float,centroid)        
    #print centroid
    #--------------------------------------------------------------------------
    ## Put the centroids into a file to share
    centroid_to_string = ','.join(map(str,centroid))
    centroid_file = open('centroids.data', 'w')     
    centroid_file.write(centroid_to_string)
    centroid_file.close()   

    #-------------------------------------------------------------------------
    # Initialization of variables
    CUs = tasks  # NOTE: Define how many CUs you are willing to use 
    convergence = False   # We have no convergence yet
    m = 0 # number of iterations
    maxIt = 5 # the maximum number of iteration
    chunk_size = total_file_lines/DIMENSIONS
    chunk_size /= CUs 
    chunk_size *= DIMENSIONS    # this is the size of the part that each unit is going to control

    # find the offsets of the lines - each cu is going to open the file on different line

    offsets = list()
    offsets.append(0)
    with open(SHARED_INPUT_FILE, "r+b") as f:
        mapped = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        i = 1
        for line in iter(mapped.readline, ""):
            if (i % chunk_size) == 0:
                offsets.append(mapped.tell())
            i+=1
    f.close()  

    #------------------------
    try:
        start_time = time.time()
        session = rp.Session() 

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # 
        # Change the user name below if you are using a remote resource 
        # and your username on that resource is different from the username 
        # on your local machine. 
        #
        # Add a Pilot Manager. Pilot managers manage one or more ComputePilots.
        pmgr = rp.PilotManager(session=session)

        # Register our callback with the PilotManager. This callback will get
        # called every time any of the pilots managed by the PilotManager
        # change their state.
        #pmgr.register_callback(pilot_state_cb)

        # ----- CHANGE THIS -- CHANGE THIS -- CHANGE THIS -- CHANGE THIS ------
        # 
        # If you want to run this example on your local machine, you don't have 
        # to change anything here. 
        # 
        # Change the resource below if you want to run on a remote resource. 
        # You also might have to set the 'project' to your allocation ID if 
        # your remote resource does compute time accounting. 
        #
        # A list of preconfigured resources can be found at: 
        # http://radicalpilot.readthedocs.org/en/latest/machconf.html#preconfigured-resources
        # 
        # define the resources you need
        pdesc = rp.ComputePilotDescription()
        pdesc.resource = resource  # NOTE: This is a "label", not a hostname
        pdesc.runtime  = int(args.walltime) # minutes
        pdesc.cores    = CPUs  # define cores 
        pdesc.cleanup  = False
        pdesc.project  = args.project
        pdesc.queue    = args.queue

        # submit the pilot.
        pilot = pmgr.submit_pilots(pdesc)
        #-----------------------------------------------------------------------
        # Define the url of the local file in the local directory
        shared_input_file_url = 'file://%s/%s' % (os.getcwd(), SHARED_INPUT_FILE)
        staged_file = "%s%s" % (MY_STAGING_AREA, 'dataset.in')

        # Configure the staging directive for to insert the shared file into
        # the pilot staging directory. - This is the dataset
        sd_pilot = {'source': shared_input_file_url, 
                    'target': staged_file, 
                    'action': rp.TRANSFER }
        # Synchronously stage the data to the pilot
        pilot.stage_in(sd_pilot)

        # Configure the staging directive for shared input file.
        sd_shared = {'source': staged_file, 
                     'target': 'dataset.in', 
                     'action': rp.LINK }

        # Combine the ComputePilot, the ComputeUnits and a scheduler via
        # a UnitManager object.
        umgr = rp.UnitManager(session)
        # Register our callback with the UnitManager. This callback will get
        # called every time any of the units managed by the UnitManager
        # change their state.
        #umgr.register_callback(unit_state_cb)

        # Add the created ComputePilot to the UnitManager.
        umgr.add_pilots(pilot)
    
        #-------------------------------------------------------------------------
        ## Staging Directives & map 

        # Staging directives for the partial sums of each Cluster
        staged_combiner_files_list = list()
        staged_combiner_files_list_input = list()

        for i in range(1,CUs+1):
            INTERMEDIATE_FILE = "combiner_file_%d.data" % i            
            # these files are output staging of the mapper and they move to the staging area for the reducer
            staged_combiner_files = {
                'source': INTERMEDIATE_FILE, 'target': 'staging:///%s' % INTERMEDIATE_FILE, 'action': rp.COPY }
            staged_combiner_files_list.append(staged_combiner_files)

            # this is the links of the combiner files for the reducer
            staged_combiner_files_link = {
             'source': 'staging:///%s' % INTERMEDIATE_FILE, 'target': INTERMEDIATE_FILE, 'action': rp.LINK }
            staged_combiner_files_list_input.append(staged_combiner_files_link)

        # the centroids are going to be staged to the staging area for the reducer - and for the new mapper iteration
        INTERMEDIATE_FILE = "centroids.data"
        centroids_output = {
                'source': INTERMEDIATE_FILE, 'target': 'staging:///%s' % INTERMEDIATE_FILE, 'action': rp.COPY }

        ## These are the links of the centroids for the input staging of  the mapper - ouput of reducer
        centroids_input = {
             'source': 'staging:///%s' % INTERMEDIATE_FILE, 'target': INTERMEDIATE_FILE, 'action': rp.LINK }
        staged_combiner_files_list_input.append('reducer.py')
        #-------------------------------------------------------------------------
        cu_set=list()
        while m<maxIt:
            ## MAPPER PHASE
            mylist = []
            for i in range(1,CUs+1):
                cudesc = rp.ComputeUnitDescription()
                cudesc.name="Mapper-{0}-{1}".format(m,i)
                cudesc.executable = "python"
                cudesc.arguments = ['mapper.py', i, k, chunk_size, CUs, DIMENSIONS, offsets[i-1], offsets[i]]
                if m==0:  # m is the number of k-means iteration - the first iteration centroids are localhost
                    cudesc.input_staging = ['mapper.py', sd_shared, 'centroids.data']
                else:
                    cudesc.input_staging = ['mapper.py', sd_shared, centroids_input ]
                cudesc.output_staging = staged_combiner_files_list[i-1]
                mylist.append(cudesc)
            mylist_units = umgr.submit_units(mylist)
            cu_set += mylist_units
            # wait for all units to finish
            umgr.wait_units()

            #-------------------------------------------------------------------------------
            # Aggregate all partial sums of each Cluster  to define the new centroids
            # here i will launch a cu
            # -------- BEGIN USER DEFINED CU DESCRIPTION --------- #
            cudesc = rp.ComputeUnitDescription()
            cudesc.name = "Reducer-{0}".format(m)
            cudesc.executable  = "python"
            cudesc.arguments   = ['reducer.py',convergence,k,DIMENSIONS,CUs]
            cudesc.input_staging = staged_combiner_files_list_input
            if m==0:
                cudesc.input_staging.append('centroids.data')
            else:
                cudesc.input_staging.append(centroids_input)
            cudesc.output_staging =  centroids_output # copy centroids to stage area
            cudesc.output_staging.append('converge.txt')
            # -------- END USER DEFINED CU DESCRIPTION --------- #
            units = umgr.submit_units(cudesc)
            cu_set.append(units)
            umgr.wait_units()
            conv = open('converge.txt')
            text = conv.readline()
            text =  text.strip()
            if text=='True':
                convergence = True
            else:
                convergence = False
            m+=1
        #--------------------END OF K-MEANS ALGORITHM --------------------------#
        # K - MEANS ended successfully - print total times and centroids
        print 'K-means algorithm ended successfully after %d iterations' % m
        print 'Converge: ',convergence
        #print 'Centroids:'
        #print units.stdout

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
        session.close(cleanup=True, terminate=True)
        
