import os
import csv
import os
import sys

def printAvg(running_total,number_of_trials,outputFile,core_count):
    avg_time = running_total/number_of_trials
    outputFile.write('Average TTC for experiments on {0} using '.format(system_name) + core_count +' cores: ' +str(avg_time) + 'sec\n\n')
    return

if __name__=="__main__":

    if len(sys.argv) !=2 :
        print 'Usage: resultReader.py <resource_name>'
        sys.exit(-1)
    else:
        system_name = sys.argv[1]

    directory = '/home/sean/midas/leaflet_finder/Spark/Spark-networkx/profiles/{0}/'.format(system_name)
    #curPath = os.path.abspath(os.curdir) + '/'
    outputFile = open('output{0}.txt'.format(system_name),'w')
    #outputFile = open('outputComet.txt','w')
    fileList = []
    for file in os.listdir(directory):
        if file.endswith('.csv'):
        	
        	fileList.append('{0}'.format(file))
        	#gather list of all .csv's
        	
    #sort them based on my naming convention
    sorted_files = sorted(fileList, key=lambda x: int(x.split('rp')[0]))

    core_count = sorted_files[0].split('rp')[0]
    number_of_trials = 0
    running_total = 0

    #read data for every file collected
    for file in sorted_files:            

        this_exp_cores = file.split('rp')[0]
        if (this_exp_cores!= core_count):
            printAvg(running_total,number_of_trials,outputFile,core_count)
            core_count=this_exp_cores
            running_total = 0
            number_of_trials = 0

        inputFile = open(directory+file)
        inputReader = csv.reader(inputFile)
        inputList = list(inputReader)
        #csv is now a list

        #stop and start points are at (2,9) and (2,10)
        done = float(inputList[2][14])
        outputStaging = float(inputList[2][10])
        executing = float(inputList[2][9])
    
        #ttc = finish-start
        #time_to_complete= outputStaging-executing
        time_to_complete = done -executing

        #write new data
        outputFile.write(file + ': time for CU to complete = ' + str(time_to_complete)+' seconds\n')
	#print "This is the current running_total: " +str(running_total)

        number_of_trials = number_of_trials+1
        running_total = running_total + time_to_complete
        if file==sorted_files[len(sorted_files)-1]:
            printAvg(running_total,number_of_trials,outputFile,core_count)

        inputFile.close()

    outputFile.close()
