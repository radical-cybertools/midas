import os
import sys
import numpy as np

from scipy import ndimage
import skimage
from skimage import feature
from skimage.color import rgb2gray
from skimage.filters import threshold_otsu, sobel
from skimage.feature import peak_local_max
from skimage.morphology import watershed
from skimage import data

import matplotlib.pyplot as plt
import matplotlib.cm as cm

from skimage import io
io.use_plugin('pil')

import argparse
import pprint
pp = pprint.PrettyPrinter().pprint


def blobDetector_analyze(image_path):
    """Runs the blob detector algorithm on the image at image_path
    
    PARAMETERS
    -----------
    image_path : string
        absolute path to the image
        
    RETURN
    -------
    numpy.array
    
    """
    def otsu_thresholding(image):
        block_size=35
        thresh = threshold_otsu(image,block_size)
        binary = image > thresh
        return binary


    def blob_detection(image):
        threshhold=0.1
        overlap=0
        blobs_dog = blob_dog(image, max_sigma=30, threshold=threshhold, overlap=overlap)
        return blobs_dog


    # read in image file
    image = rgb2gray(plt.imread(image_path))

    # create a threshhold of the image
    thresh = otsu_thresholding(image)

    # convert the image to a threshholded version
    binary = image > thresh

    # # find blobds in the binary image
    blobs = blob_detection(binary)

    fig, ax = plt.subplots(1, 1)

    # map projected blobs onto image
    for blob in blobs:
        y, x, r = blob
        c = plt.Circle((x, y), r, color="red", linewidth=1, fill=False)
        ax.add_patch(c)
        
    ax.set_axis_off()

    fig.tight_layout()

    return fig


def blobDetector_multi(path, from_image, until_image, imgext, inputs, outputs):
    """From the inputs and ouputs folder located in path, we retrieve images
    from inputs folder, run the blob detector algorithm on it, then save it
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
    
    print('Input Arguments')
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
        image_path = os.path.join(path_for_input, str(from_image) + imgext)

        fig = blobDetector_analyze(image_path)
        
        # absolute output image path
        image_path = os.path.join(path_for_output, str(from_image) + imgext)
        
        fig.savefig(image_path)

        print ' [x] saved to %s' % image_path

        from_image += 1

    return
