<h1>Watershed PySpark Installation Instructions</h1>

We use the anaconda package manager for this part of the project.

1. [Install Anaconda Instructions](https://conda.io/projects/conda/en/latest/user-guide/install/index.html)

2. Install and activate the conda environment from the environment.yml file

    ```
    conda env create -f environment.yml
    ```

    ```
    . activate watershed_Spark

    or

    conda activate watershed_Spark
    ```

3. [Install Apache Spark Driver 2.3](https://spark.apache.org/downloads.html)


4. Run the experiment.

    ```
    example: spark-submit watershed.py \
"${SCHEDULER}:${PORT}" $tasks $images /oasis/projects/nsf/unc100/willc97/  -i $input -o $output  -r $output -e $imgext
python watershed.py 24 192  4096 $project_name xsede.comet_orte_anaconda compute /path/to/input_and_output_folders --walltime 55 -i $input_folder_name -o $output_folder_name -r $report_name


usage: watershed.py [-h] [-e IMGEXT] [-b {0,1}] [-r REPORT] [-i INPUTS]
                    [-o OUTPUTS] [-v]
                    scheduler tasks images path

positional arguments:
  scheduler             master node
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
    ```
