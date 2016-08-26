from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {
    "name":         "conn_comp",                  # Mandatory
    "description":  "takes adj matrix, gives conn_comp subgraphs",        # Optional
    "arguments":   {                          # Mandatory
    	"--trajectory-count=": {
            "mandatory": True,
            "description": "number of trajectories"
            },
        "--python-script=": {
            "mandatory": True,
            "description": "script to find connected components"
            },
        "--window-size=": {
            "mandatory": True,
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
                "pre_exec"    : ['export LMOD_CMD=/opt/apps/lmod/lmod/libexec/lmod','export LMOD_SYSTEM_DEFAULT_MODULES=TACC','module restore','module load python/2.7.9', 'source /work/04059/tg833588/wrangler/radical.pilot.sandbox/ve_wrangler/bin/activate'],         # list or None, can be used to load modules
                "executable"  : ["python"],        # specify the executable to be used
                "uses_mpi"    : False         # mpi-enabled? True or False
            },
            "xsede.comet":
            {
                "environment" : None,
                "pre_exec"    : ['module load python/2.7.10', 'source /home/solejar/radical.pilot.sandbox/ve_comet/bin/activate'],
                "executable"  : ['python'],
                "uses_mpi"    : False
            }
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
        
        arguments  = ['{0}'.format(self.get_arg('--python-script=')),
                      #this is arg for script
                      int('{0}'.format(self.get_arg('--trajectory-count='))),
                      #this is input file for script
                      int('{0}'.format(self.get_arg('--window-size=')))]

        self._executable  = cfg["executable"]
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]

# ------------------------------------------------------------------------------
