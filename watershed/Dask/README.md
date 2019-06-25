<h1>Watershed Dask Installation Instructions</h1>

We use the anaconda package manager for this part of the project.

1. [Install Anaconda Instructions](https://conda.io/projects/conda/en/latest/user-guide/install/index.html)

2. Install and activate the conda environment from the environment.yml file

    ```
    conda env create -f environment.yml
    ```

    ```
    . activate watershed_Dask

    or

    conda activate watershed_Dask
    ```

3. Retrieve hostnames of nodes assigned to you

    For systems with the slurm workload manager
    ```
    hostnodes=`scontrol show hostnames $SLURM_NODELIST`
    ```

4. Start the dask-ssh server that will allow for interconnectivity between nodes

    ```
    dask-ssh --nprocs 24 --nthreads 1 $log_dir $hostnodes &
    sleep 10
    ```

5. Set environment variables

    ```
    SCHEDULER=`hostname`
    scheduler="$SCHEDULER:8786"
    ```

6. Run the experiment

    ```
    example:

    python watershed_dask.py $scheduler $tasks $images $path -i $input_folder -o $output_folder -r $report_name

    usage: watershed_dask.py [-h] [-e IMGEXT] [-b {0,1}] [-r REPORT] [-i INPUTS]
                            [-o OUTPUTS] [-v]
                            scheduler tasks images path

    positional arguments:
    scheduler             Dask Distributed Scheduler URL
    tasks                 number of tasks to submit
    images                number of images to analyze
    path                  path of data input and outputs folders

    optional arguments:
    -h, --help            show this help message and exit
    -e IMGEXT, --imgext IMGEXT
                            extension of image files being read in (defaults to
                            .jpg)
    -b {0,1}, --brightness {0,1}
                            set image background brightness (defaults to 0)
    -r REPORT, --report REPORT
                            report name used as name of session folder (defaults
                            to "watershed_report")
    -i INPUTS, --inputs INPUTS
                            inputs folder name (defaults to "inputs")
    -o OUTPUTS, --outputs OUTPUTS
                            outputs folder name (defaults to "outputs")
    -v, --verbosity       increase outputs verbosity (defaults to 2)
    ```