Welcome to leaflet finder algorithm documentation!
===========================================

Installation:
First install Radical-Pilot and its dependences. You can find out how to do it 
here:  http://radicalpilot.readthedocs.org/en/latest/installation.html#id1

You need to install numpy to your local and remote machine:

	pip install numpy --user

You need to install networkx in your remote machine:

	pip install networkx --user
 
 Download source code:

 	Download: leaflet_finder.py  atom_distances.py and find_connencted_components.py files 


As a dataset you should use an "input.txt" file

	Download: input.txt file which is a very small sample


If you are using your own dataset:
Every line can contain one atom. Every dimension is seperated by "\t" (tab) character
 
Choose a database:
example:
	
	export RADICAL_PILOT_DBURL="mongodb://localhost:27017"

Run the Code:

To give it a test drive try via command line the following command::
	
	python leaflet_finder.py
