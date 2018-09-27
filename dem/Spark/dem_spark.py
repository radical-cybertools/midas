import os
import sys
import time
import argparse 
import datetime
import glob
import csv

from pyspark import SparkContext
from pyspark import SparkConf

import pprint
pp = pprint.PrettyPrinter().pprint

import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

def dem_multi(data):
    """From the inputs and ouputs folder located in path, we retrieve images
    from inputs folder, run the dem algorithm on it, then save it
    to the outputs folder
    
    PARAMETERS
    -----------
    path : string
        absolute path to the inputs and outputs folder
    from_image : int
        images are named as %d.imgext so we start analyzing file from_image.imgext
    
    until_image : int
        images are named as %d.imgext so we stop analyzing until_image.imgext
    
    imgext : string
        specifies the extension of the images
        
    inputs : string
        name of the inputs folder inside path
        
    outputs : string
        name of the outputs folder inside path
        
    RETURN
    -------
    None
    """

    path, from_image, until_image, imgext, inputs, outputs = data
    from imageio import imwrite
    from pydem.dem_processing import DEMProcessor

    def dem_analyze(input_path, output_path):
        """Runs the dem algorithm on the image at input_path
        and saves it to output_path
        
        PARAMETERS
        -----------
        input_path : string
            absolute path to the input image

        output_path : string
            absolute path to the output image
        """
        
        dem_proc = DEMProcessor(input_path)

        mag, asp = dem_proc.calc_slopes_directions()

        #Calculate the upstream contributing area:
        uca = dem_proc.calc_uca()

        #Calculate the TWI:
        twi = dem_proc.calc_twi()

        imwrite(output_path % 'mag', mag)
        imwrite(output_path % 'asp', asp)
        imwrite(output_path % 'uca', uca)
        imwrite(output_path % 'twi', twi)

        return

    
    print 'Input Arguments'
    pp([   ['path             ' , path              ],
           ['from_image       ' , from_image        ],
           ['until_image      ' , until_image       ],
           ['imgext           ' , imgext            ],
           ['outputs          ' , outputs           ],
           ['inputs           ' , inputs            ]
       ])
    
    path_for_input = os.path.join(path, inputs)
    path_for_output = os.path.join(path, outputs)
    
    # check extension validity
    if imgext[0] != '.':
        imgext = '.' + imgext

    # inputs folder must exist
    if not os.path.isdir(path_for_input):
        raise Exception('path does not exist ' + path_for_input)

    # outputs folder can be created
    if not os.path.isdir(path_for_output):
        try:
            os.mkdir(path_for_output)
        except OSError:
            # needs to catch this error due to concurrency
            pass

    while from_image <= until_image:

        # absolute input image path
        input_path = os.path.join(path_for_input, str(from_image) + imgext)

        # absolute output image path
        output_path = os.path.join(path_for_output, str(from_image) + '%s' + imgext)

        dem_analyze(input_path, output_path)
        
        print ' [x] saved to %s' % output_path

        from_image += 1

    return
    

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    # master node
    parser.add_argument('scheduler',
                        type=str,
                        help='master node')
    # num compute units
    parser.add_argument('tasks',
                        type=int,
                        help='number of tasks to submit')
    # number images
    parser.add_argument('images',
                        type=int,
                        help='number of images to analyze')
    # data path to input and outputs folders
    parser.add_argument('path',
                        type=str,
                        help='path of data input and outputs folders')
    # image extension
    parser.add_argument('-e', '--imgext',
                        type=str,
                        default='.jpg',
                        help='extension of image files being read in (defaults to .jpg)')
    # report name
    parser.add_argument('-r', '--report',
                        type=str,       
                        default='dem_report',
                        help='report name used as name of session folder (defaults to "dem_report")')
    # inputs folder name
    parser.add_argument('-i', '--inputs',
                        type=str,       
                        default='inputs',
                        help='inputs folder name (defaults to "inputs")')
    # outputs folder name
    parser.add_argument('-o', '--outputs',
                        type=str,       
                        default='outputs',
                        help='outputs folder name (defaults to "outputs")')
    # verbosity
    parser.add_argument('-v', '--verbosity',
                        action='count', 
                        default=2,
                        help='increase outputs verbosity (defaults to 2)')

    # retrieve arguments
    args = parser.parse_args()

    # set variables
    scheduler           = args.scheduler
    number_of_tasks     = args.tasks
    number_of_images    = args.images
    report              = args.report+'.txt'
    path                = args.path
    inputs              = args.inputs
    outputs             = args.outputs
    verbosity           = args.verbosity
    if args.imgext[0] == '.': 
        imgext = args.imgext
    else :               
        imgext = '.' + args.imgext

    if verbosity >= 2:
        print('Input Arguments:')
        pp([   ['number_of_tasks  ' , number_of_tasks   ],
               ['number_of_images ' , number_of_images  ],
               ['report           ' , report            ],
               ['path             ' , path              ],
               ['inputs           ' , inputs            ],
               ['outputs          ' , outputs           ]
           ])
    if verbosity >= 1:
        print 'Arguments are valids'

    # Client timestamp start
    start_client = time.time()

    # sc = SparkContext(master=scheduler, appName="PythonDEM")
    conf = SparkConf().setAppName("PythonDEM").setMaster("spark://%s" % scheduler)
    sc = SparkContext(conf=conf)

    # Task creation start
    start_create_tasks = time.time()

    """
    Create list of arguments to submit to dem_multi
    as a list into the Client. The current implementation
    accounts for nonhomogenous images per task through an
    taking the additional load and distributing it evenly
    """

    print("Creating tasks for SparkContext scheduler")

    images_in_each_task = number_of_images / number_of_tasks
    additional_load     = number_of_images % number_of_tasks

    step = 0
    task_args = list()
    for i in xrange(number_of_tasks):

        if (not additional_load):
            args = (path,
                    step,
                    step+images_in_each_task-1,
                    imgext,
                    inputs,
                    outputs)
            step += images_in_each_task
        else:
            args = (path,
                    step,
                    step+images_in_each_task,
                    imgext,
                    inputs,
                    outputs)
            step += images_in_each_task + 1
            additional_load -= 1

        task_args.append(args)

    # Task creation stop
    stop_create_tasks = time.time()
    
    print("Successfuly created tasks for SparkContext scheduler")
    
    tasks = sc.parallelize(task_args, len(task_args)).map(dem_multi)
    
    # Compute timstamp start
    start_compute = time.time()
   
    res_stacked = tasks.collect()
   
    # Compute timestamp stop
    stop_compute = time.time()
   
    sc.stop()
   
    # Client timestamp stop
    stop_client = time.time()
   
    print("Finished pipeline")

    profile_headers = ['start_client', 'stop_client', 'start_create_tasks', 'stop_create_tasks', 'start_compute', 'stop_compute']
    profile_times  = [start_client, stop_client, start_create_tasks, stop_create_tasks, start_compute, stop_compute]

    profile_file_path = os.path.join(os.getcwd(), 'profiles_' + report[:-4]  + '.csv')
    
    with open(profile_file_path, mode='w') as profile_file:
        profile_writer = csv.writer(profile_file, delimiter=',')

        profile_writer.writerow(['headers', 'times'])
        for header, time in zip(profile_headers, profile_times):
            profile_writer.writerow([header, time])
