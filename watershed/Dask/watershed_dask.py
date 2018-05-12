import os
import sys
import time
import argparse 
import datetime
import glob

import pprint
pp = pprint.PrettyPrinter().pprint

import dask
from dask.distributed import Client
from distributed.diagnostics.plugin import SchedulerPlugin

from watershed import watershed_multi

def submitCustomProfiler(profname,dask_scheduler):
    prof = MyProfiler(profname)
    dask_scheduler.add_plugin(prof)

def removeCustomProfiler(dask_scheduler):
    dask_scheduler.remove_plugin(dask_scheduler.plugins[-1])

class MyProfiler(SchedulerPlugin):
    def __init__(self,profname):
        self.profile = profname

    def transition(self,key,start,finish,*args,**kargs):
        if start == 'processing' and finish == 'memory':
            with open(self.profile,'a') as ProFile:
                ProFile.write('{}\n'.format([key,start,finish,kargs['worker'],kargs['thread'],kargs['startstops']]))

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    # pilot cores
    parser.add_argument('scheduler',
                        type=str,
                        help='Dask Distributed Scheduler URL')
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
        pp([   ['Scheduler        ' , scheduler         ],
               ['number_of_tasks  ' , number_of_tasks   ],
               ['number_of_images ' , number_of_images  ],
               ['bright_background' , bright_background ],
               ['report           ' , report            ],
               ['path             ' , path              ],
               ['inputs           ' , inputs            ],
               ['outputs          ' , outputs           ]
           ])
    if verbosity >= 1:
        print 'Arguments are valid'


    client = Client(scheduler)

    client.run_on_scheduler(submitCustomProfiler,os.getcwd()+report)

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

    res_stacked = tasks.compute(get=client.get)
    client.run_on_scheduler(removeCustomProfiler)
    client.shutdown()
