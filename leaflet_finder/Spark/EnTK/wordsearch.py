import sys
import numpy as np
import networkx as nx
#from pyspark  import  SparkContext
from time import time

if __name__=="__main__":

    if len(sys.argv) != 3:
        print "Usage: wordsearch.py <word> <text_file>"
        exit(-1)
    else:
        word_search = sys.argv[1]
        filename = sys.argv[2]

    start_time = time()
    orig_sentenceList = []
    sorted_sentenceList = []
    word_count = 0

    search_file = open(filename, "r")
    output_file = open(filename + 'Sorted', 'w')
    for line in search_file:
        if word_search in line:
            word_count +=1
            orig_sentenceList.append(line)
        else:
            sortedLine = sorted(line.split())
            newLine = ""
            for item in sortedLine:l 
                newLine += item + ' '
            sorted_sentenceList.append(newLine)

    print_list = orig_sentenceList + sorted_sentenceList

    for item in print_list:
    	output_file.write('%s \n' % item)


    search_file.close()	
    output_file.close()



    print 'Total times word appears in document: %i times' % (word_count)
    print 'Total time to search for word count: %i sec' %(time()-start_time)