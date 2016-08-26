from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {
    "name":         "spark",                  # Mandatory
    "description":  "spark applications",        # Optional
    "arguments":   {                          # Mandatory
    	"--exec-mem=": {
            "mandatory": True,
            "description": "memory for each executor process"
            },
        "--driver-mem=": {
            "mandatory": True,
            "description": "memory for driver where Spark Context initializes"
            },
        "--max-result-size=": {
            "mandatory": True,
            "description": "max result size for Spark driver."
            },
        "--spark-script=": {
            "mandatory": True,
            "description": "Spark script in Python"
            },
        "--input-file=": {
            "mandatory": False,
            "description": "Any necessary input files"
            },
        "--partitions=": {
            "mandatory": False,
            "description": "Number of partitions in dataset, if not set here, determined by Spark."
        }
        },
    "machine_configs":                        
    #use only spark-submit for all machines.
                                              # Use a dictionary with keys as
        {                                     # resource names and values specific
            "local.localhost":                # to the resource
            {
                "environment" : None,         # dict or None, can be used to set env variables
                "pre_exec"    : None,         # list or None, can be used to load modules
                "executable"  : ["spark-submit"],        # specify the executable to be used
                "uses_mpi"    : False         # mpi-enabled? True or False
            },
            "xsede.wrangler_spark":
            {
                "environment": None,
                "pre_exec"   : ["module load java-paths"],
                "executable" : ["spark-submit"],
                "uses_mpi"   : False
            },
        }
}

# ------------------------------------------------------------------------------
#
class MyUserDefinedKernel(KernelBase):

    def __init__(self):

        super(MyUserDefinedKernel, self).__init__(_KERNEL_INFO)
     	"""Le constructor."""
        		
    # --------------------------------------------------------------------------
    #
    @staticmethod
    def get_name():
        return _KERNEL_INFO["name"]
        

    def _bind_to_resource(self, resource_key):
        """This function binds the Kernel to a specific resource defined in
        "resource_key".
        """      
        if resource_key not in _KERNEL_INFO["machine_configs"]:
            resource_key = 'local.localhost'
            
        cfg = _KERNEL_INFO["machine_configs"][resource_key]

        arguments  = ['--conf spark.driver.maxResultSize={0}'.format(self.get_arg("--max-result-size=")),
                      '--executor-memory {0}'.format(self.get_arg('--exec-mem=')),
                      '--driver-memory {0}'.format(self.get_arg('--driver-mem=')),
                      #this is script name
                      '{0}'.format(self.get_arg('--spark-script=')),
                      #this is arg for script
                      int('{0}'.format(self.get_arg('--partitions='))),
                      #this is input file for script
                      '{0}'.format(self.get_arg('--input-file='))]

        self._executable  = cfg["executable"]
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]

# ------------------------------------------------------------------------------
