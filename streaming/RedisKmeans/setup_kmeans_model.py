import os
import sys
import numpy as np


def put_model(model):
    r.set('kmeans', pickle.dumps(model))

    return

def get_model():
    return pickle.loads(r.get("kmeans"))


#### Configurations ######
number_centroids = 10
number_dimensions = 3
### Configurations ######



redis_host = sys.argv[1]

r = redis.StrictRedis(host=redis_host, port=6379, db=0)


centroids = np.random.randn(number_centroids, number_dimensions)
kmeans = sklearn.cluster.MiniBatchKMeans(n_clusters=len(centroids), init=centroids, n_init=1).fit(centroids)
put_model(kmeans)


