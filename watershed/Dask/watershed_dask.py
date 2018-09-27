import os
import sys
import time
import argparse 
import datetime
import glob
import csv

import dask
from dask.distributed import Client
from distributed.diagnostics.plugin import SchedulerPlugin

import pprint
pp = pprint.PrettyPrinter().pprint


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


def watershed_multi(path, from_image, until_image, brightness, imgext, inputs, outputs):
    """From the inputs and ouputs folder located in path, we retrieve images
    from inputs folder, run the watershed algorithm on it, then save it
    to the outputs folder
    
    PARAMETERS
    -----------
    path : string
        absolute path to the inputs and outputs folder
    from_image : int
        images are named as %d.imgext so we start analyzing file from_image.imgext
    
    until_image : int
        images are named as %d.imgext so we stop analyzing until_image.imgext
    
    brightness : int 
        defines the background color using values [0, 1]
    
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

    from scipy import ndimage
    from skimage import feature
    from skimage.color import rgb2gray
    from skimage.filters import threshold_otsu, sobel
    from skimage.feature import peak_local_max
    from skimage.morphology import watershed

    import numpy as np

    from matplotlib import pyplot

    from skimage import io
    io.use_plugin('pil')

    def watershed_analyze(image_path, brightness):
        """Runs the watershed algorithm on the image at image_path
        
        PARAMETERS
        -----------
        image_path : string
            absolute path to the image
        
        brightness : integer
            defines the background color using values [0, 1]
            
        RETURN
        -------
        numpy.array
        
        """
        img = pyplot.imread(image_path)

        img_gray = rgb2gray(img)

        # return threshold value based on on otsu's method
        thresh = threshold_otsu(img_gray)                   

        if brightness:
            foreground_mask = img_gray <= thresh            # for bright background
        else:
            foreground_mask = img_gray > thresh             # for dark background


        # compute the Euclidean distance from every binary pixel to the nearest zero pixel 
        # and then find peaks in this distance map
        distance = ndimage.distance_transform_edt(foreground_mask)

        # return a boolean array shaped like image, with peaks represented by True values
        localMax = peak_local_max(distance, indices=False, min_distance=30, labels=foreground_mask)

        # perform a connected component analysis on the local peaks using 8-connectivity 
        markers = ndimage.label(localMax, structure=np.ones((3, 3)))[0]

        # apply the Watershed algorithm
        labels = watershed(-distance, markers, mask=foreground_mask)

        print ' [x] Analyzing image %s' % (image_path)
        print ' [x] there are %d segments found' % (len(np.unique(labels)) - 1)

        # loop over the unique labels returned by the Watershed algorithm
        # each label is a unique object
        img.setflags(write=1)
        for label in np.unique(labels):

            # if the label is zero, we are examining the 'background' so ignore it
            if label != 0:

                # create a black mask  
                mask = np.zeros(img_gray.shape, dtype="uint8")

                # it make the pixels that correspond to the label-object white
                mask[labels == label] = 255         

                # compute the sobel transform of the mask to detect the label's-object's edges
                edge_sobel = sobel(mask)

                # make all the pixel, which correspond to label's edges, green in the image                 
                img[edge_sobel > 0] = [0,255,0]

        return img
    
    print('Input Arguments')
    pp([   ['path             ' , path              ],
           ['from_image       ' , from_image        ],
           ['until_image      ' , until_image       ],
           ['brightness       ' , brightness        ],
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

        # absolute image path
        image_path = os.path.join(path_for_input, str(from_image) + imgext)

        img = watershed_analyze(image_path, brightness)
        
        name_out_image = os.path.join(path_for_output, str(from_image) + imgext)
        io.imsave(name_out_image, img)

        print ' [x] saved to %s' % name_out_image

        from_image += 1

    return
    

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

    # Client timestamp start
    start_client = time.time()

    client = Client(scheduler)
    client.run_on_scheduler(submitCustomProfiler,os.getcwd()+'/'+report)

    # Task creation start
    start_create_tasks = time.time()

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
            step += images_in_each_task
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

    # Task creation stop
    stop_create_tasks = time.time()

    tasks = [dask.delayed(watershed_multi)(*args) for args in task_args]

    # Compute timestamp start
    start_compute = time.time()

    res_stacked = dask.compute(tasks, get=client.get)

    # Compute timestamp stop
    stop_compute = time.time()

    client.run_on_scheduler(removeCustomProfiler)
    client.close()

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
