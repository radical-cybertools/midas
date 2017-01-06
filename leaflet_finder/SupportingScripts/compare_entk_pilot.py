import os
import csv
import os
import sys

if __name__=="__main__":

    directory_enTK = '/home/sean/midas/leaflet_finder/Vanilla/profiles/comet/'
    directory_RP = '/home/sean/midas/leaflet_finder/Vanilla/profiles/comet_vanilla/'
    #curPath = os.path.abspath(os.curdir) + '/'
    
    #outputFile = open('outputComet.txt','w')
    fileList_enTK = []
    fileList_RP = []
    
    current_core = ''
    for file in os.listdir(directory_enTK):
        if file.endswith('.prof'):
            no_copy = 1
            for elem in fileList_enTK:
                if elem.split('rp')[0]==file.split('rp')[0]:
                    no_copy = 0
            if no_copy:
                fileList_enTK.append('{0}'.format(file))
           
        	#gather list of all .prof's
    
    current_core = ''
    for file in os.listdir(directory_RP):
        if file.endswith('.prof'):
            no_copy = 1
            for elem in fileList_RP:
                if elem.split('rp')[0]==file.split('rp')[0]:
                    no_copy = 0
            if no_copy:
                fileList_RP.append('{0}'.format(file))
            
            #gather list of all .prof's        
        	
    #sort them based on my naming convention
    sorted_files_entk = sorted(fileList_enTK, key=lambda x: int(x.split('rp')[0]))
    sorted_files_rp = sorted(fileList_RP, key=lambda x: int(x.split('rp')[0]))

    cur_rp = 0
    cur_entk = 0
    
    print str(len(sorted_files_entk))
    print str(len(sorted_files_rp))
    for count, file in enumerate(sorted_files_entk):
        outputFile = open('difference_vanilla{0}.txt'.format(str(count+1)),'w')
        input_file_entk = open(directory_enTK+file)
        next(input_file_entk)
        input_file_rp = open(directory_RP+sorted_files_rp[count])
        next(input_file_rp)
        
        time_list = []
        print file
        print sorted_files_rp[count]
        for line in input_file_entk:
            state_entk = line.split(':')[1]
            time_entk = float(line.split(',')[0])
            #print state_entk
            for lineRP in input_file_rp:
                
                state_rp = lineRP.split(':')[1]
                #print state_entk
                #print state_rp
                time_rp = float(lineRP.split(',')[0])
                if state_entk==state_rp:
                    
                    
                    time_dif = (time_entk-cur_entk)-(time_rp-cur_rp)
                    
                    #print 'found a match!'
                    if time_dif > 0.1 or time_dif < -0.1:
                        time_line = '\nt delta for ' + state_entk.rstrip() + ': ' + str(time_dif) 
                        time_list.append(time_line)                        
                    
                cur_rp = time_rp    
            
            cur_entk = time_entk

            input_file_rp.seek(0,0)
            next(input_file_rp)

        sorted_times_list= sorted(time_list, key=lambda x: float(x.split(':')[1]), reverse=True)
        for elem in sorted_times_list:
            outputFile.write(elem)

        input_file_entk.close()
        input_file_rp.close()
        outputFile.close()
