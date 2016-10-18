import os
import sys
import time

if __name__=="__main__":

    if len(sys.argv) != 2:
        print "Usage: sleep.py <time_to_sleep>"
        exit(-1)
    else:
        sleep_time = float(sys.argv[1])
	time.sleep(sleep_time)


