#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <math.h>
#include <qcprot.h>
#include <sys/time.h>

using namespace std;

double rmsd(double** xref){

    double **xmobile;
    double temp;
    double *pointer = new double[9];

    xmobile = new double*[3];
    
    for (int i=0;i<3;i++){
        xmobile[i] = new double[146];
        for (int j=0;j<146;j++){
            temp = rand();
            xmobile[i][j] = (temp/RAND_MAX)*15;
        }
    }

    temp = CalcRMSDRotationalMatrix(xref,xmobile,146,pointer,NULL);

    return temp;
};

double *block_rmsd(double **xref0, int start, int stop,int step,int rank){

    int bsize;
    double *results;

    double *t_comp;

    bsize = stop - start;

    results = new double[stop-start];
    for (int i=0;i<stop-start;i+=step){
        results[i] = rmsd(xref0);
    }

    return results;

};


int main (){
    int rank;
    int ierr;
    int size;
//
//  Initialize MPI.
//
    ierr = MPI_Init ( NULL, NULL );
//
//  Get the number of processes.
//
    ierr = MPI_Comm_size ( MPI_COMM_WORLD, &size );
//
//  Get the individual process ID.
//
    ierr = MPI_Comm_rank ( MPI_COMM_WORLD, &rank );



    int nframes=1024; //total number of frames
    int bsize; // number of frames of this process
    double *results; // pointer to the RMSD results array from all processes
    double **xref0; //Reference Frame pointer. It is a 3 by 3341
    int start,stop; //first and last frame that will be used in this process
    double *result;  //pointer to RMSD result array per process.
    long int tstart,tstop; // Variable that will hold timestamps for timing values.
    struct timeval tp; //Time struct.



    int *duration = new int[3]; //keeps track of durations for each process
    int *durations = new int[size*3]; // Pointer that will gather all the information

    // Get timestamp and convert it to ms
    gettimeofday(&tp, NULL);
    tstart = tp.tv_sec * 1000 + tp.tv_usec / 1000;

    // Find the number of frames each process will calculate the RMSD.
    bsize = ceil(nframes/size);

    cout<<"Hello from rank "<<rank<<" with data size of "<<bsize<< "\n";

    start = rank*bsize;
    stop = (rank+1)*bsize;

     // Generation of the random reference frame
    xref0 = new double*[3];
    double a = rand();

    for (int i=0;i<3;i++){
        xref0[i] = new double[3341];
        for (int j=0;j<3341;j++){
            a = rand();
            xref0[i][j] = (a/RAND_MAX)*15;
        }
    }


    result = new double[bsize];
    // Get init time 
    gettimeofday(&tp, NULL);
    tstop = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    duration[0] = tstop-tstart;
    tstart = tstop;
    // Main computation function for each process. Gets an array.
    result = block_rmsd(xref0,start,stop,1,rank);

    // Compute duration
    gettimeofday(&tp, NULL);
    tstop = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    duration[1] = tstop-tstart;
    tstart = tstop;

    // Gather all the results to rank 0. the 'if' statement is commented out to replicate
    // the exact gather Mahzad executed in her code. There might be a redundancy since 
    // all processes are allocating the space for the results.
    /*if (rank==0){*/
    results = new double[size*bsize];
    MPI_Gather(result,bsize,MPI_DOUBLE,results,bsize,MPI_DOUBLE,0,MPI_COMM_WORLD);
    /*}
    else{
        MPI_Gather(result,bsize,MPI_DOUBLE,NULL,0,MPI_DOUBLE,0,MPI_COMM_WORLD);
    }*/

    // Gather duration
    gettimeofday(&tp, NULL);
    tstop = tp.tv_sec * 1000 + tp.tv_usec / 1000;
    duration[2] = tstop-tstart;


    // Gather duration timings from all processes
    MPI_Gather(duration,3,MPI_INT,durations,3,MPI_INT,0,MPI_COMM_WORLD);

    //if ( rank == 0 )
      //{
        //Gather the results and store them in file. No MPI IO
      //}
    //cout << "Bye from rank "<<rank<<"\n";

    MPI_Finalize ( );

    // write results to a file as well as the durations. The reason for the write is in case
    // some optimization is done to the compiler, we need to make sure it will not discard
    // the calcualtions because the space with the results is not used.
    
    if (rank==0){
        ofstream myfile;
        myfile.open ("example.txt");
        myfile << "Writing this to a file.\n";
        cout << "Writing to example.txt\n";
        for(int count = 0; count < nframes; count ++){
            myfile << results[count]<< "\n" ;
        }
        myfile.close();
        myfile.open ("timings.csv");
        myfile << "Rank,Init,Execute,Gather\n";
        for (int i=0;i<size;i++)
            myfile<<i<<","<<durations[i*3]<<","<<durations[i+3+1]<<","<<durations[i*3+2]<<"\n";
        myfile.close();

    }
    return 0;
}
