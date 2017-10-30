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
            xmobile[i][j] = (temp/RAND_MAX)*15;
        }
    }

    temp = CalcRMSDRotationalMatrix(xref,xmobile,146,pointer,NULL);
    cout <<temp<<"\n";
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


    int bsize;
    double **xref0;
    int start,stop;
    double *result;

    bsize = ceil(nframes/size);

    cout<<"Hello from rank "<<rank<<" with data size of "<<bsize<< "\n";

    start = rank*bsize;
    stop = (rank+1)*bsize;

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
    result = block_rmsd(xref0,start,stop,1);

    for (int i=0;i<bsize;i++)
        cout<<"RANK "<<rank<<"RMSD :"<<i<<","<<result[i]<<"\n";
//
//  Process 0 prints an introductory message.
//

    if (rank==0){
        cout<<"Setting up the gathering variable\n";
            results = new double[nframes];}

    MPI_Barrier(MPI_COMM_WORLD);

    MPI_Gather(result,4,MPI_REAL,results,4,MPI_REAL,0,MPI_COMM_WORLD);
    //if ( rank == 0 )
      //{
        //Gather the results and store them in file. No MPI IO
      //}
    cout << "Bye from rank "<<rank<<"\n";

    MPI_Finalize ( );
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
}
