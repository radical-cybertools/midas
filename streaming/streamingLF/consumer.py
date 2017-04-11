from pykafka import KafkaClient
import networkx as nx
import sys

def pairwise_distance_balltree(points_np):
    tree = BallTree(points_np, leaf_size=40)
    edges = tree.query_radius(points_np, 15.0)
    edge_list=[list(zip(np.repeat(idx, len(dest_list)), dest_list)) for idx, dest_list in enumerate(edges)]
    edge_list_flat = np.array([list(item) for sublist in edge_list for item in sublist])
    res = edge_list_flat
    res=edge_list_flat[edge_list_flat[:,0]<edge_list_flat[:,1], :]
    return res

def pairwise_distance_kdtree(points_np):
    tree = KDTree(points_np, leaf_size=40)
    edges = tree.query_radius(points_np, 15.0)
    edge_list=[list(zip(np.repeat(idx, len(dest_list)), dest_list)) for idx, dest_list in enumerate(edges)]
    edge_list_flat = np.array([list(item) for sublist in edge_list for item in sublist])
    res = edge_list_flat
    res=edge_list_flat[edge_list_flat[:,0]<edge_list_flat[:,1], :] 
    return res

def pairwise_distance_cdist(points_np, cutoff=15.0):
    distances = cdist(points_np, points_np)
    true_res = np.array(np.where(distances < cutoff))
    res=np.array(zip(true_res[0], true_res[1]))
    res=res[res[:,0]<res[:,1], :]
    return res


broker = sys.argv[1]

print broker

print client.topics


topic = client.topics['atoms']
consumer = topic.get_simple_consumer()


for message in consumer:
    if message is not None:
        print message.offset, message.value
    else:
        print 'None'

'''
while True:
    message = consumer.consume(block=True)
    #print message.value
    now = time.time()
    #sent_time=datetime.datetime.strptime(message.value, "%Y-%m-%dT%H:%M:%S.%fZ")
    sent_time_string = message.value.split(";")[0]
    sleep_time =float(message.value.split(";")[1])
    sent_time = dateutil.parser.parse(sent_time_string)
    sent_time_ts = time.mktime(sent_time.timetuple())
    lat = now-sent_time_ts - TIME_OFFSET   
    result = "kafka, latency, 0, %.5f, %.5f, %s, %s\n"%(1/sleep_time, lat, message.value.split(";")[0], 
                                                                                datetime.datetime.now().isoformat())
    print(result)
'''

graph = nx.from_edgelist(res_tree)
subgraphs = nx.connected_components(graph)
indices = [np.sort(list(g)) for g in subgraphs]



