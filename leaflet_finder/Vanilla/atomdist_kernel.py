from radical.ensemblemd.kernel_plugins.kernel_base import KernelBase

# ------------------------------------------------------------------------------
#
_KERNEL_INFO = {
    "name":         "atom_dist",                  # Mandatory
    "description":  "takes adj matrix, gives conn_comp subgraphs",        # Optional
    "arguments":   {                          # Mandatory
        "--python-script=": {
            "mandatory": True,
            "description": "script to find connected components"
            },
        "--traj-count=": {
            "mandatory": True,
            "description": "number of trajectories"
            },
        "--window-size=": {
            "mandatory": True,
            "description": "Number of partitions in dataset, if not set here, determined by Spark."
            },
        "--cutoff=": {
            "mandatory": True,
            "description": "this is the cutoff for connection" 
            },
        "--row=": {
            "mandatory": True,
            "description": "this is the row size"
            },        
        "--column=": {
            "mandatory": True,
            "description": "this is the column size"
            },
        },    
    "machine_configs":                        
    #use only spark-submit for all machines.
                                              # Use a dictionary with keys as
        {                                     # resource names and values specific
            "local.localhost":                # to the resource
            {
                "environment" : None,         # dict or None, can be used to set env variables
                "pre_exec"    : ['module load python/2.7.9'],         # list or None, can be used to load modules
                "executable"  : ["python"],        # specify the executable to be used
                "uses_mpi"    : False,         # mpi-enabled? True or False
                "cores"       : 4
            },
            "xsede.comet":
            {
                "environment" : None,
                "pre_exec"    : ['module load python/2.7.10'],
                "executable"  : ['python'],
                "uses_mpi"    : False,
                "cores"       : 4
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
                      #this the script to run
                      int('{0}'.format(self.get_arg('--window-size='))),
                      #this is size of partition for AllPairs
                      int('{0}'.format(self.get_arg('--row='))),
                      #this is current row in partition
                      int('{0}'.format(self.get_arg('--column='))),
                      #this is current column in partition
                      int('{0}'.format(self.get_arg('--traj-count='))),
                      #this is the amount of atoms
                      int('{0}'.format(self.get_arg('--cutoff=')))
                      #this is the cutoff for connectedness
                      ]

        self._executable  = cfg["executable"]
        self._arguments   = arguments
        self._environment = cfg["environment"]
        self._uses_mpi    = cfg["uses_mpi"]
        self._pre_exec    = cfg["pre_exec"]
        self.__cores      = cfg["cores"]

# ------------------------------------------------------------------------------
