import os, sys
import commands
import radical.pilot as rp
import random
import pandas as pd

import redis
import numpy as np
import time
import datetime
import dateutil.parser
import ast
import sklearn.cluster
import pickle


#run_timestamp=datetime.datetime.now()
#RESULT_FILE= "results/kafka-throughput-consumer-pilot-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"
#try:
#    os.makedirs("results")
#except:
#    pass
#output_file=open(RESULT_FILE, "w")


def print_details(detail_object):
    if type(detail_object)==str:
        detail_object = ast.literal_eval(detail_object)
    for i in detail_object:
        detail_object[i]=str(detail_object[i])
    return pd.DataFrame(detail_object.values(), index=detail_object.keys(), columns=["Value"])



os.environ["RADICAL_PILOT_VERBOSE"]="ERROR"
os.environ["RADICAL_SAGA_PTY_VERBOSE"]="ERROR" 
#os.environ["RADICAL_PILOT_DBURL"]="mongodb://mongo.radical-cybertools.org:24242/sc15-test000"
#os.environ["RADICAL_PILOT_DBURL"]="mongodb://c251-120:27017/sc15-test000"
session = rp.Session()
c = rp.Context('ssh')
c.user_id = "tg829618"
session.add_context(c)

pmgr = rp.PilotManager(session=session)
umgr = rp.UnitManager (session=session)
print "Session id: %s Pilot Manager: %s" % (session.uid, str(pmgr.as_dict()))

pdesc = rp.ComputePilotDescription()
pdesc.resource = "xsede.wrangler_streaming"  # NOTE: This is a "label", not a hostname
pdesc.runtime  = 20 # minutes
pdesc.cores    = 24
pdesc.cleanup  = False
pdesc.project = "TG-MCB090174"
pdesc.queue = 'debug'
pdesc.access_schema = 'gsissh'

pilot = pmgr.submit_pilots(pdesc)
umgr.add_pilots(pilot)

#----------BEGIN USER DEFINED TEST-CU DESCRIPTION-------------------#
cudesc = rp.ComputeUnitDescription()
cudesc.executable = 'python'
cudesc.arguments = ['test.py']
cudesc.input_staging = ['test.py']
cudesc.cores =1
#-----------END USER DEFINED TEST-CU DESCRIPTION--------------------#

print 'Starting up Kafka cluster..'
cu_set = umgr.submit_units([cudesc])
umgr.wait_units()
print 'Kafka cluster is running'


# Benchmarks

# configuration
number_centroids = 10
number_dimensions = 3
number_messages = 1000
number_cus = 1
repeats = 3


def put_model(model):
    r.set('kmeans', pickle.dumps(model))
        
def get_model():
    return pickle.loads(r.strget("kmeans"))


centroids = np.random.randn(number_centroids, number_dimensions)
kmeans = sklearn.cluster.MiniBatchKMeans(n_clusters=len(centroids), init=centroids, n_init=1)


per_cu_messages = number_messages/number_cus
cudesc_list=[]

pilot_info = pilot.as_dict()
pilot_info = pilot_info['resource_details']['lm_detail']
ZK_URL = pilot_info['zk_url']
redis_URL = broker = pilot_info['brokers'][0]
print "ZK_URL: %s " % ZK_URL

r = redis.StrictRedis(host=redis_URL, port=6379, db=0)

for i in range(number_cus):
    cudesc = rp.ComputeUnitDescription()
    cudesc.executable  = "python"
    cudesc.arguments   = [os.path.join(os.getcwd(), 'rp_kmeans_streaming.py'), 100, 1, 1,ZK_URL,redis_URL]
    cudesc.cores       = 1
    cudesc_list.append(cudesc)

