#midas - Leaflet Finder using Spark API


Requirements:

Spark Leaflet Finder requires:

1. Apache Spark :  http://spark.apache.org/downloads.html
2. Graph Frame Package : http://graphframes.github.io
3. scipy, numpy


Download Source code:

    Download: leafletfinder.py
    

Dataset:

    Leafletfinder requires npz.npy file as input file 


Run the Code:

    python $SPARK_HOME/bin/spark-submit --packages graphframes:graphframes:0.1.0-spark1.6 leafletfinder.py <input-file>



