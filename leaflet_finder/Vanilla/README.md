Welcome to leaflet finder algorithm documentation!
===========================================

Installation:
First install Radical-Pilot and its dependences. You can find out how to do it 
here:  http://radicalpilot.readthedocs.org/en/latest/installation.html#id1

You need to install numpy to your local machine:
```
	pip install numpy --user
```
Download source code:

 	Download: leaflet_finder.py  atom_distances.py and find_connencted_components.py files 


Datasets can be foud in the dataset folders.

If you are using your own dataset, it should be a numpy array with dimensions nX3,
where n is the number of atoms in your system.
 
Choose a database:
example:
	
	export RADICAL_PILOT_DBURL="mongodb://localhost:27017"

Run the Code:
```
	python leaflet_finder.py win_size cores atomFile atomNum session
```
Doing:
```
     python leaflet_finder.py -h
```
prints

```
usage: leaflet_finder.py [-h] win_size cores atomFile atomNum session

positional arguments:
  win_size    Dimension size of the submatrix
  cores       Number of cores that will be requested
  atomFile    The numpy file that contains the positions of the atoms
  atomNum     The totatl number of atoms in the atomFile
  session     The RADICAL-Pilot Session name

optional arguments:
  -h, --help  show this help message and exit
```
