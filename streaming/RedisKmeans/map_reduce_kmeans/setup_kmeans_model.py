import sys
import pickle
import redis
import numpy as np


def save_clusters_to_redis(clusters):
    """
    - Add docstring
    """
    r.set('means', pickle.dumps(clusters))

    return
#### Configurations ######
number_centroids = 10
number_dimensions = 3
### Configurations ######



redis_host = sys.argv[1]

r = redis.StrictRedis(host=redis_host, port=6379, db=0)


centroids = np.random.randn(number_centroids, number_dimensions)
save_clusters_to_redis(centroids)


