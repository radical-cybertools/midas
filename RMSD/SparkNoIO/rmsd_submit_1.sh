#!/bin/bash
#SBATCH -J RMSDSpark1  # Job name
#SBATCH -o RMSDSpark1.out   # Name of stdout output file(%j expands to jobId)
#SBATCH -e RMSDSpark1.err   # Name of stderr output file(%j expands to jobId)
#SBATCH -p normal
#SBATCH -N 2                # Total number of nodes requested (16 cores/node)
#SBATCH -n 2                # Total number of mpi tasks requested
#SBATCH -t 01:00:00         # Run time (hh:mm:ss) - 1.5 hours
# The next line is required if the user has more than one project
#SBATCH -A  # Allocation name to charge job against

module load jdk64/1.8.0

SCHEDULER=`hostname`
echo SCHEDULER: $SCHEDULER
sleep 5

hostnodes=`scontrol show hostnames $SLURM_NODELIST`
echo $hostnodes

cd /work/03170/tg824689/wrangler/spark-2.2.1-bin-hadoop2.7/conf

echo "Slaves:"
>slaves
for host  in ${hostnodes:8}; do
    echo $host
    echo $host >>slaves
done

echo 'spark.master spark://'$SCHEDULER':7077' > spark-defaults.conf

echo 'export JAVA_HOME=/usr/java/jdk1.8.0_92' > spark-env.sh
echo 'export PYSPARK_PATH=/data/03170/tg824689/anaconda2/bin/python' >>spark-env.sh
echo 'export SPARK_MASTER_HOST='$SCHEDULER >> spark-env.sh
echo 'export SPARK_MASTER_PORT=7077'>>spark-env.sh
echo 'export SPARK_WORKER_CORES=24'>>spark-env.sh

cd /work/03170/tg824689/wrangler/spark-2.2.1-bin-hadoop2.7/sbin
./start-all.sh
cd ../bin
export PATH=$PWD:$PATH
cd /data/03170/tg824689/Beckstein2/SparkNoIO/

spark-submit --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=./ --conf spark.ui.port=4045 --conf spark.driver.maxResultSize=50g --executor-memory 60g --driver-memory 60g RMSD_Spark_random_array.py $SCHEDULER 24

mkdir 1node
mv app* 1node


