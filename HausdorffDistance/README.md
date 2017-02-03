# Execution commands:

## RADICAL-Pilot Vanilla:

Initially open the file ```rp_hausdorff_opt.py``` and set the following values accordingly
```
Line 11: NUMBER_OF_TRAJECTORIES : The total number of trajectories that will be used for the computation
Line 12: ATOM_SEL: Type of the atom. Valid values 'ca', 'ha' or 'aa'
line 13: TRAJ_SIZE: The Size of the trajectory. Valid values 'short', 'med' or 'long'
Line 15: TRJ_LOCATION :Path that points to the folder the trajectories are
```

#### Command:
```
python rp_hausdorff_opt.py <window_size> <cores> <session_name>
```
#### Argument explanation:
```
<window_size> : The number of trajectories that will be used for each independent calculation. 
              This number must divide the total number of trajectories 
<cores>       : The total number of cores RADICAL-Pilot will use
<session_name>: The name of the RADICAL-Pilot session.
```

## RADICAL-Pilot Spark command:
```
python rp_hausdorff_sp.py <session_name> <cores> <NumOfTrj> <Sel> <Size> <WindowSize> <TrjLocation>
```
### Argument explaination:

```
<session_name>: RP Session name. It can be any string
<cores>: The number of cores that are going to be used. Note that one node will be used for the Spark Master
<NumOfTrj>: The total number of trajectories.
<Sel>: Type of the Atom of the trajectories. Possible values so far 'ca', 'ha' and 'aa'
<Size>: The size of the trajectory. It can be short,med or long
<WindowSize>: The number of trajectories that will be used for each independent calculation. 
              This number must divide the total number of trajectories
<TrjLocation>: The folder where the trajectory files are on the target resource.
```

# Data Location:
#### Stampede:
```
/scratch/03170/tg824689/trj_RADICAL/converted/
```

#### Comet:
```
/oasis/projects/nsf/unc100/iparask/trj_RADICAL/
```
There are folders named ```ca```,```ha``` and ```aa``` for the respective atom type.
