#!/bin/bash
#SBATCH -J DaskRMSD1  # Job name
#SBATCH -o DaskRMSD1.out   # Name of stdout output file(%j expands to jobId)
#SBATCH -e DaskRMSD1.err   # Name of stderr output file(%j expands to jobId)
#SBATCH -p normal
#SBATCH -N 1                # Total number of nodes requested (16 cores/node)
#SBATCH -n 1                # Total number of mpi tasks requested
#SBATCH -t 08:00:00         # Run time (hh:mm:ss) - 1.5 hours
# The next line is required if the user has more than one project
#SBATCH -A  # Allocation name to charge job against

cd $SLURM_SUBMIT_DIR

source activate MDAnalysis

SCHEDULER=`hostname`
echo SCHEDULER: $SCHEDULER
hostnodes=`scontrol show hostnames $SLURM_NODELIST`
echo $hostnodes

dask-ssh --nprocs 24 --nthreads 1 --log-directory ./ $hostnodes &
sleep 20
echo "====-get to work-===="
python DaskDistNoIO.py $SCHEDULER:8786 24 $SLURM_SUBMIT_DIR
echo "===- Kill Scheduler -==="
ps axf | grep dask-ssh | grep -v grep | awk '{print "kill -9 " $1}' | sh

mkdir 1node
mv *.txt 1node
