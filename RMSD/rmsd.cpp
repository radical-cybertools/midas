#include <stdio>
#include <mpi.h>
#include <stdlib.h>
#include <iostream>

using namespace std;


double *rmsd(int* xref){
    double xmobile[146][3];

    for (int i=0;i<146;i++){
        for (int j=0;j<3;j++)
            xmobile[i][i] = rand()*15;
    }

    return CalcRMSDRotationalMatrix(xref,xmobile,146);
}

double **block_rmsd(int *xref0, int start, int stop,int step){

    int bsize;
    double **results;

    double *t_comp;

    cout << "BlockRMSD"<<start<<stop<<step<<"\n";
    bsize = stop - start;

    for (int i=start;i<stop;i+=step){
        result[i] = rmsd(xref0);

    }

    return result;

}


void main(){
    int rank;
    int ierr;
    int size;
    double wtime;
    int nframes;
    double bsize;
    double xref0[3341][3];
    double **result;
    int start,stop;
    float **results = NULL;
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


    for (int i=0;i<3341;i++){
        for (int j=0;j<3;j++){
            xref0[i][j] = rand()*15;
        }
    }

    result = block_rmsd(xref,start,stop,1);

//
//  Process 0 prints an introductory message.
//

    MPI_Gather(&result,sizeof(result)/sizeof(float),MPI_FLOAT,results,sizeof(result)/sizeof(float),MPI_FLOAT,0,MPI_COMM_WORLD);
    if ( rank == 0 )
      {
        //Gather the results and store them in file. No MPI IO
      }

    MPI_Finalize ( );
}