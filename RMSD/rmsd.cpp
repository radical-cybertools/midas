#include <stdio.h>
//#include <mpi.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <math.h>
#include <qcprot.h>

using namespace std;


double rmsd(double** xref){

    double **xmobile;
    double temp;

    xmobile = new double*[146];

    for (int i=0;i<146;i++){
        xmobile[i] = new double[3];
        for (int j=0;j<3;j++){
            temp = rand();
            xmobile[i][j] = temp/RAND_MAX;
        }
    }

    return  CalcRMSDRotationalMatrix(xref,xmobile,146,NULL,NULL);
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
    int size=1;
    double wtime;
    int nframes=2;
    double bsize;
    double **xref0;
    int start,stop;
    double *results;
//
//  Initialize MPI.
//
    //ierr = MPI_Init ( NULL, NULL );
//
//  Get the number of processes.
//
    //ierr = MPI_Comm_size ( MPI_COMM_WORLD, &size );
//
//  Get the individual process ID.
//
    //ierr = MPI_Comm_rank ( MPI_COMM_WORLD, &rank );

    bsize = ceil(nframes/size);

    start = rank*bsize;
    stop = (rank+1)*bsize;

    xref0 = new double*[3341];
    results = new double[nframes];
    cout << "RAND_MAX: "<< RAND_MAX<<"\n";
    double a = rand();

    for (int i=0;i<3341;i++){
        xref0[i] = new double[3];
        for (int j=0;j<3;j++){
            a = rand();
            xref0[i][j] = a/RAND_MAX;
        }
    }

    results = block_rmsd(xref0,start,stop,1);

//
//  Process 0 prints an introductory message.
//

    //MPI_Gather(&result,sizeof(result)/sizeof(float),MPI_FLOAT,results,sizeof(result)/sizeof(float),MPI_FLOAT,0,MPI_COMM_WORLD);
    //if ( rank == 0 )
      //{
        //Gather the results and store them in file. No MPI IO
      //}

    ofstream myfile;
    myfile.open ("example.txt");
    myfile << "Writing this to a file.\n";
    cout << "Writing to exampe.txt\n";
    for(int count = 0; count < nframes; count ++){
        myfile << results[count]<< "\n" ;
    }
    myfile.close();

    return 0;
    //MPI_Finalize ( );
}