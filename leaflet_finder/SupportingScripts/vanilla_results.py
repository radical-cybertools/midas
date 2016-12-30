import os
import csv
import os
import sys

def printAvg(running_total,number_of_trials,outputFile,core_count):
    avg_ttc = running_total[0]/number_of_trials
    avg_adjmatrix = running_total[1]/number_of_trials
    avg_conncomp = running_total[2]/number_of_trials
    outputFile.write('Average TTC, adj_matrix, conn_comp on {0} using '.format(system_name) 
    + core_count +' cores: {0}, {1}, {2} sec\n\n'.format(str(avg_ttc),str(avg_adjmatrix),str(avg_conncomp)))
    return

if __name__=="__main__":

    if len(sys.argv) !=2 :
        print 'Usage: resultReader.py <resource_name>'
        sys.exit(-1)
    else:
        system_name = sys.argv[1]

    directory = '/home/sean/midas/leaflet_finder/Vanilla/profiles/{0}/'.format(system_name)
    #curPath = os.path.abspath(os.curdir) + '/'
    outputFile = open('output{0}_vanilla.txt'.format(system_name),'w')
    #outputFile = open('outputComet.txt','w')
    fileList = []
    for file in os.listdir(directory):
        if file.endswith('.prof'):
        	
        	fileList.append('{0}'.format(file))
        	#gather list of all .prof's
        	
    #sort them based on my naming convention
    sorted_files = sorted(fileList, key=lambda x: int(x.split('rp')[0]))

    core_count = sorted_files[0].split('rp')[0]
    number_of_trials = 0
    running_total = [0,0,0]
    #contains running total of each value I'm collecting

    #read data for every file collected
    for file in sorted_files:            

        this_exp_cores = file.split('rp')[0]
        if (this_exp_cores!= core_count):
            printAvg(running_total,number_of_trials,outputFile,core_count)
            core_count=this_exp_cores
            running_total = [0,0,0]
            number_of_trials = 0

        inputFile = open(directory+file)
        next(inputFile)

        for line in inputFile:
            line_segs = line.split(':')
            #print line_segs[0]
            line_right_split = line_segs[1].split(',')
            line_left_split = line_segs[0].split(',')
            if line_right_split[2]=='Executing':
                time_start_atomdist = float(line_left_split[0])
                break

        inputFile.seek(0)
        next(inputFile)

        for line in inputFile:
            line_segs = line.split(':')
            line_right_split = line_segs[1].split(',')
            
            if line_right_split[1]=='unit.000378':
                if line_right_split[2]=='Executing':
                    line_left_split = line_segs[0].split(',')
                    time_start_conncomp = float(line_left_split[0])
                elif line_right_split[2]=='Done':
                    line_left_split = line_segs[0].split(',')
                    time_end = float(line_left_split[0])
                    break               
    
        #ttc = finish-start
        time_to_complete  = time_end-time_start_atomdist
        time_to_adj_matrix= time_start_conncomp-time_start_atomdist
        time_to_conncomp  = time_end - time_start_conncomp

        #write new data
        outputFile.write(file + ': time for CU to complete = ' + str(time_to_complete)+' seconds, \n')
	#print "This is the current running_total: " +str(running_total)

        number_of_trials = number_of_trials+1

        running_total[0] = running_total[0] + time_to_complete
        running_total[1] = running_total[1] + time_to_adj_matrix
        running_total[2] = running_total[2] + time_to_conncomp
    
        if file==sorted_files[len(sorted_files)-1]:
            printAvg(running_total,number_of_trials,outputFile,core_count)

        inputFile.close()

    outputFile.close()
