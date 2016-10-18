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

#window_size = None
window_list = []
traj_count = None
instance_count= None

#make your globals!!

#Now carry on with your application as usual !

class leaflet(SimulationAnalysisLoop):
    def __init__(self,iterations, simulation_instances, analysis_instances):

        SimulationAnalysisLoop.__init__(self,iterations,simulation_instances,analysis_instances)

    def simulation_stage(self,iteration,instance):

        k= Kernel(name="atom_dist")
        #do this with instance param
        k.arguments=['--python-script=atom_distances_gimped.py','--traj-count={0}'.format(str(traj_count)),
                     '--window-size={0}'.format(str(window_size)),'--cutoff=15',
                     '--row={0}'.format(str(instance-1)),
                     '--column=-1']
        k.link_input_data=['$SHARED/input.txt > input.txt']
        k.upload_input_data=['atom_distances_gimped.py']
        outputFile = 'distances_{0}.npz.npy'.format(str(instance-1))
        k.copy_output_data=['{0} > $SHARED/{0}'.format(outputFile)]
        return k   

    def analysis_stage(self,iteration,instance):

        k=Kernel(name="conn_comp")
        k.arguments =['--trajectory-count={0}'.format(str(traj_count)),'--python-script=find_connencted_components_gimped.py',
                      '--window-size={0}'.format(str(window_size))]
        k.upload_input_data=['find_connencted_components_gimped.py']
        k.link_input_data=[]
        for elem in range(0,instance_count):
            inputFile = 'distances_{0}.npz.npy'.format(str(elem))
            k.link_input_data.append('$SHARED/{0} > {0}'.format(inputFile))

        return k

# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.

        if len(sys.argv) !=4:
            print "Usage error: lf_finder.py <traj_filename> <cores>"
            sys.exit(-1)
        else:
            window_size = int(sys.argv[3])
            core_count = int(sys.argv[2])
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
                walltime=90,
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


        instance_count = int(math.ceil(float(traj_count)/float(window_size)))
        print "instance total is " + str(instance_count)

        leaflet = leaflet(iterations=1,simulation_instances=instance_count,analysis_instances=1)

        cluster.run(leaflet)

        #cluster.profile(leaflet)

        cluster.deallocate()



    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace

