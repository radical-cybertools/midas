import os
import sys
import numpy as np

from scipy import ndimage
from skimage import feature
from skimage.color import rgb2gray
from skimage.filters import threshold_otsu, sobel
from skimage.feature import peak_local_max
from skimage.morphology import watershed

from matplotlib import pyplot

from skimage import io
io.use_plugin('pil')

import argparse
import pprint
pp = pprint.PrettyPrinter().pprint


#-------------------------------------------------------------------------------
parser = argparse.ArgumentParser()

# data path
parser.add_argument('path',
                    type=str,
                    help='path of data input and output folders')
# start from this image #i.jpg
parser.add_argument('from_image',
                    type=int,
                    help='start from this image number')
# stop after this image #f.jpg
parser.add_argument('until_image',
                    type=int,
                    help='go until this image number')
# background brightness
parser.add_argument('brightness',
                    type=int, 
                    choices=[0, 1],
                    help='set brightness of image background')
# image extension
parser.add_argument('imgext',
                    type=str, 
                    help='extension of image files being read in')
# inputs folder name
parser.add_argument('inputs',
                    type=str,       
                    help='inputs folder name')
# outputs folder name
parser.add_argument('outputs',
                    type=str,       
                    help='outputs folder name')
# verbosity
parser.add_argument('-v', '--verbosity',
                    action='count', 
                    default=2,
                    help='increase output verbosity (defaults to 2)')

# retrieve arguments
args = parser.parse_args()

path                = os.path.abspath(args.path)
read_from           = args.from_image
read_until          = args.until_image
bright_background   = args.brightness
imgext              = args.imgext
outputs             = args.outputs
inputs              = args.inputs

if args.verbosity >= 2:
    print('Input Arguments')
    pp([   ['path             ' , path              ],
           ['read_from        ' , read_from         ],
           ['read_until       ' , read_until        ],
           ['bright_background' , bright_background ],
           ['imgext           ' , imgext            ],
           ['outputs          ' , outputs           ],
           ['inputs           ' , inputs            ]
       ])
if args.verbosity >= 1:
    print 'Arguments are valid'

path_for_input = os.path.join(path, inputs)
path_for_output = os.path.join(path, outputs)

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


while read_from <= read_until:
    
    image_name = os.path.join(path_for_input, str(read_from) + imgext)

    # img = io.imread(image_name)
    img = pyplot.imread(image_name)


    img_gray = rgb2gray(img)

    thresh = threshold_otsu(img_gray)                   # return threshold value based on on otsu's method

    if bright_background:
        foreground_mask = img_gray <= thresh            # for bright background
    else:
        foreground_mask = img_gray > thresh             # for dark background


    # compute the Euclidean distance from every binary pixel to the nearest zero pixel 
    # and then find peaks in this distance map
    #
    distance = ndimage.distance_transform_edt(foreground_mask)

    # return a boolean array shaped like image, with peaks represented by True values
    localMax = peak_local_max(distance, indices=False, min_distance=30, labels=foreground_mask)

    # perform a connected component analysis on the local peaks using 8-connectivity 
    markers = ndimage.label(localMax, structure=np.ones((3, 3)))[0]
    
    # apply the Watershed algorithm
    labels = watershed(-distance, markers, mask=foreground_mask)

    print ' [x] Analyzing image %s' % (image_name)
    print ' [x] there are %d segments found' % (len(np.unique(labels)) - 1)


    # loop over the unique labels returned by the Watershed algorithm
    # each label is a unique object
    img.setflags(write=1)
    for label in np.unique(labels):
    
        # if the label is zero, we are examining the 'background' so ignore it
        #
        if label != 0:

            mask = np.zeros(img_gray.shape, dtype="uint8")       # create a black mask  
            mask[labels == label] = 255         # it make the pixels that correspond to the label-object white

            # compute the sobel transform of the mask to detect the label's-object's edges
            edge_sobel = sobel(mask)
            # make all the pixel, which correspond to label's edges, green in the image                 
            img[edge_sobel > 0] = [0,255,0]

    name_out_image = os.path.join(path_for_output, str(read_from) + imgext)
    io.imsave(name_out_image, img)

    print ' [x] saved to %s' % name_out_image

    read_from += 1

#-------------------------------------------------------------------------------
