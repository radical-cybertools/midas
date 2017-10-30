#include <stdio.h>
#include <mpi.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <math.h>
#include <qcprot.h>

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
            xmobile[i][j] = temp/RAND_MAX;
        }
    }

    temp = CalcRMSDRotationalMatrix(xref,xmobile,146,pointer,NULL);
    
    return temp;
};

double *block_rmsd(double **xref0, int start, int stop,int step){

    int bsize;
    double *results;

    double *t_comp;

    cout << "BlockRMSD "<<start<<" "<<stop<<" "<<step<<"\n";
    bsize = stop - start;

    results = new double[stop-start];
    for (int i=start;i<stop;i+=step){
        results[i] = rmsd(xref0);

    }

    return results;

};


int main(){
    int rank;
    int ierr;
    int size;
    double wtime;
    int nframes=8;
    double bsize;
    double **xref0;
    int start,stop;
    double *result;
    double *results;
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

    bsize = ceil(nframes/size);

    start = rank*bsize;
    stop = (rank+1)*bsize;

    xref0 = new double*[3];
    results = new double[nframes];
    double a = rand();

    for (int i=0;i<3;i++){
        xref0[i] = new double[3341];
        for (int j=0;j<3341;j++){
            a = rand();
            xref0[i][j] = a/RAND_MAX;
        }
    }

    result = new double[bsize]
    result = block_rmsd(xref0,start,stop,1);

//
//  Process 0 prints an introductory message.
//

    MPI_Gather(result,bsize,MPI_FLOAT,results,bsize,MPI_FLOAT,0,MPI_COMM_WORLD);
    //if ( rank == 0 )
      //{
        //Gather the results and store them in file. No MPI IO
      //}

    if (rank==0){
        ofstream myfile;
        myfile.open ("example.txt");
        myfile << "Writing this to a file.\n";
        cout << "Writing to example.txt\n";
        for(int count = 0; count < nframes; count ++){
            myfile << results[count]<< "\n" ;
        }
        myfile.close();
    }

    return 0;
    //MPI_Finalize ( );
}