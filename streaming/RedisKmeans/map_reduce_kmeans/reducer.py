import numpy as np
import redis



def get_and_aggregate_partial_sums_from_redis(clusters):
    """
    - Each worker is writing the partial sums to redis server and now I
    - Aggregate the partial sums from all the workers, in order to calculate the new centroids

    """
    aggregated_sums_of_elements = np.zeros(clusters.shape)  # agggregate sum for each centroid - shape[1] is dim of element
    n_elements_per_cluster = np.zeros((clusters.shape[0],1))   # number of elements that belongs to each centroid
    entries = r.llen('partial_sums')    # number of entries from workers

    for i in xrange(entries):
        serialized_value = r.lindex('partial_sums', i)
        apartial_sum = pickle.loads(serialized_value)
        n_elements_per_cluster = apartial_sum[0][0]   
        aggregated_sums+= apartial_sum[0][1]  ## adds up the element to all correct cluster 

    # delete entries
    r.delete('partial_sums')

    return  (aggregated_sum,n_elements_per_cluster)



def find_new_centers(data,centroids):
    """
    Add docstring
    """
    return   np.divide(data[0],data[1]) 


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









