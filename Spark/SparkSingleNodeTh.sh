#!/bin/bash
#SBATCH -J sparkNoCluster_1  # Job name
#SBATCH -o sparkNoCluster_1.out   # Name of stdout output file(%j expands to jobId)
#SBATCH -e sparkNoCluster_1.err   # Name of stderr output file(%j expands to jobId)
#SBATCH -p normal
#SBATCH -N 1                # Total number of nodes requested (16 cores/node)
#SBATCH -n 1                # Total number of mpi tasks requested
#SBATCH -t 08:00:00         # Run time (hh:mm:ss) - 1.5 hours
# The next line is required if the user has more than one project
#SBATCH -A   # Allocation name to charge job against
#SBATCH --mail-user=
#SBATCH --mail-type=begin  # email me when the job starts
#SBATCH --mail-type=end    # email me when the job finishes


SCHEDULER=`hostname`
echo SCHEDULER: $SCHEDULER
sleep 5


export SPARK_LOCAL_IP=127.0.0.1
export SPARK_HOME=/work/03170/tg824689/wrangler/SparkOverall2/spark-2.1.1-bin-hadoop2.7/
export PATH=$PATH:/work/03170/tg824689/wrangler/SparkOverall2/spark-2.1.1-bin-hadoop2.7/bin/
export PYSPARK_PYTHON=/home/03170/tg824689/miniconda2/bin/python
export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.1-src.zip

cd /work/03170/tg824689/wrangler/SparkOverall2/
./spark-2.1.1-bin-hadoop2.7/sbin/start-all.sh

python SparkSingleNodeTh.py $SCHEDULER

./spark-2.1.1-bin-hadoop2.7/sbin/stop-all.sh