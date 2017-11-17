import numpy as np
import redis



def get_and_aggregate_partial_sums_from_redis():
"""
- Each worker is writing the partial sums to redis server and now I
- Aggregate the partial sums from all the workers, in order to calculate the new centroids

"""
    return



def find_new_centers(partial_sums):

    new_clusters = np.zeros(partial_sums.shape[0],dtype=float)
    
    i = 0
    for partial_sums, number_of_el in partial_sums:
        if number_of_el!=0:
            new_clusters[i] = float(partial_sums)/number_of_el
        else:
            new_clusters[i] = partial_sums # case where only the cluster center belongs to that cluster
        i+=1

    return

def save_clusters_to_redis(clusters):

    return




if __name__='__main__':


  partial_sums = get_and_aggregate_partial_sums_from_redis()
  centroids = find_new_centers(partial_sums)
  save_clusters_to_redis(centroids)









