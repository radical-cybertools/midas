#!/bin/bash
#SBATCH -J spark_1  # Job name
#SBATCH -o spark_1.out   # Name of stdout output file(%j expands to jobId)
#SBATCH -e spark_1.err   # Name of stderr output file(%j expands to jobId)
#SBATCH -p normal
#SBATCH -N 2                # Total number of nodes requested (16 cores/node)
#SBATCH -n 2                # Total number of mpi tasks requested
#SBATCH -t 08:00:00         # Run time (hh:mm:ss) - 1.5 hours
# The next line is required if the user has more than one project
#SBATCH -A   # Allocation name to charge job against

#SBATCH --mail-user=
#SBATCH --mail-type=begin  # email me when the job starts
#SBATCH --mail-type=end    # email me when the job finishes


module load jdk64/1.8.0
SCHEDULER=`hostname`
echo SCHEDULER: $SCHEDULER
sleep 5

hostnodes=`scontrol show hostnames $SLURM_NODELIST`
echo $hostnodes

cd /work/03170/tg824689/wrangler/SparkOverall2/spark-2.1.1-bin-hadoop2.7/conf

echo "Slaves:"
>slaves
for host  in ${hostnodes:8}; do
    echo $host
    echo $host >>slaves
done

echo 'spark.master spark://'$SCHEDULER':7077' > spark-defaults.conf

echo 'export JAVA_HOME=/usr/java/jdk1.8.0_92' > spark-env.sh
echo 'export PYSPARK_PATH=/home/03170/tg824689/miniconda2/bin/python' >> spark-env.sh
echo 'export SPARK_MASTER_IP='$SCHEDULER >> spark-env.sh

cd /work/03170/tg824689/wrangler/SparkOverall2/spark-2.1.1-bin-hadoop2.7/sbin
./start-all.sh
cd /work/03170/tg824689/wrangler/SparkOverall2/

/work/03170/tg824689/wrangler/SparkOverall2/spark-2.1.1-bin-hadoop2.7/bin/spark-submit --conf spark.driver.maxResultSize=50g --executor-memory 60g --driver-memory 60g PilotSparkSingleNodeTh.py

cd /work/03170/tg824689/wrangler/SparkOverall2/spark-2.1.1-bin-hadoop2.7/sbin
./stop-all.sh