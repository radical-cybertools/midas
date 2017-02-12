# Hausdorff_opt README:
THe purpose of this README file is to show how to use the script that is executed by
the Compute Units in the RADICAL-Pilot script.

## Setup:

Include in the folder the trajectory files of the trajectories that the Hausdorff distance will be calculated.

Example:
'Element_set 1': Files trj_ca_001.npz.npy, trj_ca_002.npz.npy
'Element_set 2': Files trj_ca_010.npz.npy, trj_ca_011.npz.npy

## Command:
```
python hausdorff_opt.py "element_set1" "element_set2" "sel" "size"
```

Example:
```
python hausdorff_opt.py [1,2] [10,11] ca short
```

## Argument explanation:
```
<element_set1> : The first Set of trajectories that will be used
<element_set2> : The second set of trajectories that will be used
<sel> : Enter aa, ha, or ca for atom selection
<size> : Trajectories length. Valid Values: short,med,long. Default Value: short
    
```

After the script is run it returns a Numpy file named ```distances.npz.npy``` that contains the result of the calculation.
