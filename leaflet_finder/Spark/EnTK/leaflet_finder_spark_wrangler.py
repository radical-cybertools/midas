from radical.ensemblemd import Kernel
from radical.ensemblemd import BagofTasks
from radical.ensemblemd import EnsemblemdError
from radical.ensemblemd import SingleClusterEnvironment

#Used to register user defined kernels
from radical.ensemblemd.engine import get_engine

#Import our new kernel
from spark_kernel import MyUserDefinedKernel

# Register the user-defined kernel with Ensemble MD Toolkit.
get_engine().add_kernel_plugin(MyUserDefinedKernel)


#Now carry on with your application as usual !
class LeafletFind(BagofTasks):

    def __init__(self,stages,instances):
        BagofTasks.__init__(self, stages,instances)

    def stage_1(self, instances):
        """This step sleeps for 60 seconds."""

        k = Kernel(name="spark")
        k.upload_input_data = ['leafletfinder.py','traj_positions.npy']
        k.arguments = ["--exec-mem=60g","--driver-mem=30g",
        "--max-result-size=25g","--spark-script=leafletfinder.py","--input-file=traj_positions.npy","--partitions=378"]
        return k


# ------------------------------------------------------------------------------
#
if __name__ == "__main__":

    try:
        # Create a new static execution context with one resource and a fixed
        # number of cores and runtime.
        cluster = SingleClusterEnvironment(
                resource="xsede.wrangler_spark",
                cores=240,
                walltime=90,
        	   username="tg833588",
        	    project="TG-MCB090174",
                #queue='debug',
                database_url="mongodb://sean:1234@ds019678.mlab.com:19678/pilot_test"
        	)

        # Allocate the resources.
        cluster.allocate()

        # Set the 'instances' of the pipeline to 16. This means that 16 instances
        # of each pipeline step are executed.
        #
        # Execution of the 16 pipeline instances can happen concurrently or
        # sequentially, depending on the resources (cores) available in the
        # SingleClusterEnvironment.
        leaflet = LeafletFind(stages=1,instances=1)

        cluster.run(leaflet)

        cluster.deallocate()

    except EnsemblemdError, er:

        print "Ensemble MD Toolkit Error: {0}".format(str(er))
        raise # Just raise the execption again to get the backtrace
