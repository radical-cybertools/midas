<h1>Watershed Radical-Pilot Installation Instructions</h1>

We use the anaconda package manager for this part of the project.

1. [Install Anaconda Instructions](https://conda.io/projects/conda/en/latest/user-guide/install/index.html)

2. Install and activate the conda environment from the environment.yml file

    ```
    conda env create -f environment.yml
    ```

    ```
    . activate watershed_rp

    or

    conda activate watershed_rp
    ```

3. Create a mongoDB server. We use mlabs for this step.

4. Add the following to your .bashrc or .bash_profile or a bash script before you run the experiment

    ```
    export RADICAL_PILOT_DBURL="insert mongodb url here"
    export RADICAL_PILOT_VERBOSE=DEBUG
    export RADICAL_PILOT_PROFILE=TRUE
    export RADICAL_PROFILE=TRUE
    ```

5. Run the experiment.

    ```
    example: python watershed.py 24 192  4096 $project_name xsede.comet_orte_anaconda compute /path/to/input_and_output_folders --walltime 55 -i $input_folder_name -o $output_folder_name -r $report_name


    usage: watershed.py [-h] [-e IMGEXT] [-w WALLTIME] [-b {0,1}] [-r REPORT]
                        [-i INPUTS] [-o OUTPUTS] [-v]
                        cores cu images project resource queue path

    positional arguments:
    cores                 number of cores to use
    cu                    number of CUs to submit
    images                number of images to analyze
    project               project to obtain allocations from
    resource              resource to use
    queue                 queue to use
    path                  path of data input and outputs folders

    optional arguments:
    -h, --help            show this help message and exit
    -e IMGEXT, --imgext IMGEXT
                            extension of image files being read in (defaults to
                            .jpg)
    -w WALLTIME, --walltime WALLTIME
                            specify the walltime in minutes (defaults to 15)
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
