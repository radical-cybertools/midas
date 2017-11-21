import numpy as np
import redis



def get_and_aggregate_partial_sums_from_redis(clusters):
    """
    - Each worker is writing the partial sums to redis server and now I
    - Aggregate the partial sums from all the workers, in order to calculate the new centroids

    """
    aggregated_sums = np.zeros(clusters.shape[1])  # agggregate sum for each centroid
    n_elements_per_cluster = np.zeros((clusters.shape[0],1))   # number of elements that belongs to each centroid
    entries = r.llen('partial_sums')    # number of entries from workers
    for i in xrange(entries):
        serialized_value = r.lindex('partial_sums', i)
        apartial_sum = pickle.loads(serialized_value)
        n_elements_per_cluster = apartial_sum[1]   # TODO: Fix dimensions, probably [:,1]
        aggregated_sums+= apartial_sum[0]  ## adds up the element to all correct cluster  dim: [:,0]

    # delete entries
    r.delete('partial_sums')

    return  aggregated_sums



def find_new_centers(partial_sums):
    """
    Add docstring
    """

    new_clusters = np.zeros(partial_sums.shape[0], dtype=float)
    i = 0
    for partial_sums, number_of_el in partial_sums:
        if number_of_el != 0:
            new_clusters[i] = float(partial_sums)/number_of_el
        else:
            new_clusters[i] = partial_sums # case where only the 
                                           #cluster center belongs to that cluster
        i += 1

    return

def save_clusters_to_redis(clusters):
    """
    - Add docstring
    """
    r.set('means', pickle.dumps(clusters))

    return




if __name__ == '__main__':


    redis_host = sys.argv[1]

    r = redis.StrictRedis(host=redis_host, port=6379, db=0)
    partial_sums = get_and_aggregate_partial_sums_from_redis()
    centroids = find_new_centers(partial_sums)
    save_clusters_to_redis(centroids)









