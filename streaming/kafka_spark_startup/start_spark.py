import os, subprocess, sys, hostlist

def get_slurm_allocated_nodes():
    
    hosts = os.environ.get("SLURM_NODELIST") 
    hosts=hostlist.expand_hostlist(hosts)
    freenodes = []
    for h in hosts:
        freenodes.append((h + "\n"))
    return list(set(freenodes))

def configure_slaves():

    nodes = get_slurm_allocated_nodes()
    spark_nodes = len(nodes) - spark_count
    slaves = nodes[1+spark_nodes:]    # 1st node is spark master. ( if only one node is requested both master
    print "Spark slaves are: %s \n" % slaves    # and slave are on the same node
    spark_full_path = os.path.join(os.getcwd(),spark_home)
    slaves_file_dir = os.path.join(spark_full_path,'conf/slaves')
    slaves_file = open(slaves_file_dir,'w')
    slaves_file.write(nodes[0])   # write the master node as slave
    for slave_node in slaves:
        slaves_file.write(slave_node)
    slaves_file.close()
    return

def configure_env_variables():
    nodes = get_slurm_allocated_nodes()
    master_ip = nodes[0]
    adir = os.path.join(os.getcwd(),spark_home+'/conf')
    #spark_defaults
    spark_default = open(adir + 'spark-defaults.conf','w')
    spark_default.write('spark.master spark://%s:7077\n'%master_ip)
    spark_default.close()
    #spark-env file
    spark_env = open(adir + 'spark.env.sh','w')
    spark_env.write('export PYSPARK_PATH=/home/03662/tg829618/anaconda2/bin/python\n')
    spark_env.write('export SPARK_MASTER_HOST=%s\n'%master_ip)
    spark_env.write('export JAVA_HOME=/usr/java/jdk1.8.0_92')
    spark_env.close()
    return



def start_spark():
    configure_slaves()
    spark_full_path = os.path.join(os.getcwd(),spark_home)
    spark_command = os.path.join(spark_full_path,'sbin/start-all.sh')
    subprocess.check_output(spark_command.split())
    return 


#########################################################
#  main                                                 #
#########################################################
if __name__ == "__main__" :

    spark_count = int(sys.argv[1])
    spark_home = 'spark-2.2.0-bin-hadoop2.7'
    start_spark()
