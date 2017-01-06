from radical.ensemblemd import Kernel
#from radical.ensemblemd import BagofTasks
from radical.ensemblemd import SimulationAnalysisLoop
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment
import math
import numpy as np
import sys

#Used to register user defined kernels
from radical.ensemblemd.engine import get_engine

#Import our new kernel
from conncomp_kernel import MyUserDefinedKernel

# Register the user-defined kernel with Ensemble MD Toolkit.
get_engine().add_kernel_plugin(MyUserDefinedKernel)

from atomdist_kernel import MyUserDefinedKernel
get_engine().add_kernel_plugin(MyUserDefinedKernel)

window_size = None
window_list = []
traj_count = None

#make your globals!!

#Now carry on with your application as usual !

class leaflet(SimulationAnalysisLoop):
    def __init__(self,iterations, simulation_instances, analysis_instances):

        SimulationAnalysisLoop.__init__(self,iterations,simulation_instances,analysis_instances)

    def simulation_stage(self,iteration,instance):

        k= Kernel(name="atom_dist")
        #do this with instance param
        k.arguments=['--python-script=atom_distances.py','--traj-count={0}'.format(str(traj_count)),
                     '--window-size={0}'.format(str(window_size)),'--cutoff=15',
                     '--row={0}'.format(str(window_list[instance-1][0]+1)),
                     '--column={0}'.format(str(window_list[instance-1][1]+1))]
        k.link_input_data=['$SHARED/input.txt > input.txt']
        k.upload_input_data=['atom_distances.py']
        outputFile = 'distances_{0}_{1}.npz.npy'.format(str(window_list[instance-1][0]),str(window_list[instance-1][1]))
        k.copy_output_data=['{0} > $SHARED/{0}'.format(outputFile)]
        return k   

    def analysis_stage(self,iteration,instance):

        k=Kernel(name="conn_comp")
        k.arguments =['--trajectory-count={0}'.format(str(traj_count)),'--python-script=find_connencted_components.py',
                      '--window-size={0}'.format(str(window_size))]
        k.upload_input_data=['find_connencted_components.py']
        k.link_input_data=[]
        for elem in window_list:
            inputFile = 'distances_{0}_{1}.npz.npy'.format(str(elem[0]),str(elem[1]))
            k.link_input_data.append('$SHARED/{0} > {0}'.format(inputFile))

        return k

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.

        if len(sys.argv) != 4:
            print "Usage error: lf_finder.py <traj_filename> <window_size> <cores>"
            sys.exit(-1)
        else:
            core_count = int(sys.argv[3])
            window_size = int(sys.argv[2])
            traj_filename = sys.argv[1]

        traj_numpy = np.load(traj_filename)
        traj_count = traj_numpy.shape[0]

        data=open('input.txt','w')
        
        for coordinates in traj_numpy:
            data.write('%s,%s,%s\n'%(coordinates.tolist()[0],coordinates.tolist()[1],coordinates.tolist()[2]))
        
        data.close()

        cluster = SingleClusterEnvironment(
                resource="xsede.comet",
                cores=core_count,
                walltime=60,
                username="solejar",
                project="unc100",
                #queue='debug',
                database_url="mongodb://sean:1234@ds019678.mlab.com:19678/pilot_test"
            )

        cluster.shared_data =[
            '/home/sean/midas/leaflet_finder/Vanilla/input.txt'
        ]

        
        
        # Allocate the resources.
        cluster.allocate()
        #stage input data???

        #make list of every window combination, to be used in atomDist
        
        #for i in range(0,traj_count,window_size):
         #   for j in range(i, traj_count-1,window_size):

          #      list_elem = [i,j]
           #     window_list.append(list_elem)
        
        #instanceCount= len(window_list)

        instanceCount = traj_count
        
        print "instance total is " + str(instanceCount)

        leaflet = leaflet(iterations=1,simulation_instances=instanceCount,analysis_instances=1)

        cluster.run(leaflet)

        cluster.deallocate()



    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace

