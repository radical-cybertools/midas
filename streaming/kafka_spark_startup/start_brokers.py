import sys,os, hostlist, socket, pkg_resources, logging




def get_server_properties(master, hostname, broker_id):

    filename = 'kafka_2.11-0.8.2.1/config/server.properties_example'
    my_data = pkg_resources.resource_string(__name__, filename)
    my_data = my_data%(master, hostname, hostname, broker_id)
    my_data = os.path.expandvars(my_data)
    return my_data

def get_slurm_allocated_nodes():
    
    hosts = os.environ.get("SLURM_NODELIST") 
    hosts=hostlist.expand_hostlist(hosts)
    freenodes = []
    for h in hosts:
        freenodes.append((h + "\n"))
    return list(set(freenodes))


def configure_kafka():
    master = socket.gethostname().split(".")[0]
    nodes = get_slurm_allocated_nodes() 
    # 1st node is spark master and zookeeper instance
    b_nodes = nodes[1:1+broker_count]   
    print 'Brokers: %s' % b_nodes

    for idx, node in enumerate(b_nodes):
        path = os.path.join(apath, "broker-%d"%idx)
        if not os.path.exists(path):
            os.makedirs(path)
        server_properties_filename = os.path.join(path, "server.properties")
        server_properties_file = open(server_properties_filename, "w")
        server_properties_file.write(get_server_properties(master=master, hostname=node.strip(), broker_id=idx))
        server_properties_file.close()
    return b_nodes

def start_brokers(broker_nodes):

    cur_dir = os.getcwd()
    kafka_home_full = cur_dir + '/kafka_2.11-0.8.2.1'

    for idx,node in enumerate(broker_nodes):
        path = os.path.join(apath, "broker-%d"%idx)
        path = os.path.join(path, "server.properties")
        start_command = "ssh " + node.strip() + " " + kafka_home_full + "/bin/kafka-server-start.sh" + \
                                        " -daemon " + cur_dir + '/' + path 
        logging.info("Execute: %s"%start_command)
        os.system(start_command)
    






#########################################################
#  main                                                 #
#########################################################
if __name__ == "__main__" :

    broker_count = int(sys.argv[1])
    kafka_home = 'kafka_2.11-0.8.2.1'
    apath = 'kafka_2.11-0.8.2.1/brokers'
    broker_nodes = configure_kafka()
    start_brokers(broker_nodes)
