import os
import sys
import time
import argparse 
import datetime

import pprint
pp = pprint.PrettyPrinter().pprint

import dask
from dask.distributed import Client, LocalCluster
from dask_jobqueue import SLURMCluster

from watershed import watershed_multi


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    # pilot cores
    parser.add_argument('cores',
                        type=int,
                        help='number of cores to use')
    # num compute units
    parser.add_argument('tasks',
                        type=int,
                        help='number of tasks to submit')
    # number images
    parser.add_argument('images',
                        type=int,
                        help='number of images to analyze')
    # project
    parser.add_argument('project',
                        type=str,
                        help='project to obtain allocations from')
    # resource
    parser.add_argument('resource',
                        type=str,
                        choices=['local', 'slurm'],
                        help='resource to use')
    # queue
    parser.add_argument('queue',
                        type=str,
                        help='queue to use')
    # data path to input and outputs folders
    parser.add_argument('path',
                        type=str,
                        help='path of data input and outputs folders')
    # image extension
    parser.add_argument('-e', '--imgext',
                        type=str,
                        default='.jpg',
                        help='extension of image files being read in (defaults to .jpg)')
    # walltime
    parser.add_argument('-w', '--walltime',
                        type=int,
                        default=15,
                        help='specify the walltime in minutes (defaults to 15)')
    # brightness background
    parser.add_argument('-b', '--brightness',
                        type=int,
                        default=0, 
                        choices=[0, 1],
                        help='set image background brightness (defaults to 0)')
    # report name
    parser.add_argument('-r', '--report',
                        type=str,       
                        default='watershed_report',
                        help='report name used as name of session folder (defaults to "watershed_report")')
    # outputs folder name
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
    cores               = args.cores
    number_of_tasks     = args.tasks
    number_of_images    = args.images
    project             = args.project
    resource            = args.resource
    queue               = args.queue
    walltime            = str(datetime.timedelta(minutes=args.walltime))
    bright_background   = args.brightness
    report              = args.report
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
        pp([   ['number_of_cores  ' , cores             ],
               ['number_of_tasks  ' , number_of_tasks   ],
               ['number_of_images ' , number_of_images  ],
               ['project          ' , project           ],
               ['resource         ' , resource          ],
               ['queue            ' , queue             ],
               ['walltime         ' , walltime          ],
               ['bright_background' , bright_background ],
               ['report           ' , report            ],
               ['path             ' , path              ],
               ['inputs           ' , inputs            ],
               ['outputs          ' , outputs           ]
           ])
    if verbosity >= 1:
        print 'Arguments are valid'


    """
    Create a client server using the appropriate cluster
    specified by the user
    """

    # TODO: submit tasks to HPC using cluster
    # not obvious how to do this with delayed

    # if resource == "slurm":
    #     cluster = SLURMCluster(queue=queue,
    #                            project=project,
    #                            walltime=walltime,
    #                            job_cpu=number_of_cores)
    # else:
    #     cluster = LocalCluster()

    # client = Client(cluster)

    """
    Create list of arguments to submit to watershed_multi
    as a list into the Client. The current implementation
    accounts for nonhomogenous images per task through an
    taking the additional load and distributing it evenly
    """

    images_in_each_task = number_of_images / number_of_tasks
    additional_load     = number_of_images % number_of_tasks

    step = 0
    task_args = list()
    for i in xrange(number_of_tasks):

        if (not additional_load):
            args = (path,
                    step,
                    step+images_in_each_task-1,
                    bright_background,
                    imgext,
                    inputs,
                    outputs)
        else:
            args = (path,
                    step,
                    step+images_in_each_task,
                    bright_background,
                    imgext,
                    inputs,
                    outputs)
            step += images_in_each_task + 1
            additional_load -= 1

        task_args.append(args)

    tasks = [watershed_multi(*args) for args in task_args]

    dask.compute(tasks)

