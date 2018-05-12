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

import pprint
pp = pprint.PrettyPrinter().pprint

from dask.delayed import delayed

def watershed_analyze(image_path, brightness):
    """
    DOCSTRING
    ----------
    Runs the watershed algorithm on the image at image_path
    
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


@delayed
def watershed_multi(path, from_image, until_image, brightness, imgext, inputs, outputs):
    """
    DOCSTRING
    ----------
    From the inputs and ouputs folder located in path, we retrieve images
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
    