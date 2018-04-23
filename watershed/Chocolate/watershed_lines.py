import os
import sys
import numpy as np

from scipy import ndimage
from skimage import feature
from skimage.color import rgb2gray
from skimage.filters import threshold_otsu, sobel
from skimage.feature import peak_local_max
from skimage.morphology import watershed

from skimage import io
io.use_plugin('pil')



#-------------------------------------------------------------------------------

if (len(sys.argv)<3) or (len(sys.argv)>4):
	print "Usage: python %s <index of first image to process> <index of last image to process> \
				<backround> - default bright (use 'd' for 'dark')." % __file__ 
	print "Please run the script again!"
	sys.exit(-1)

elif (len(sys.argv)==4) and (sys.argv[3]=='d'):
	bright_backround = 0

else:
	bright_backround = 1


# path = '/oasis/scratch/comet/statho/temp_project/Dataset_16GB/'
# FIXME
# Use for testing on Will's local linux VM
path = '/Users/WillC/Documents/Rutgers/Research/RADICAL/watershed/midas/watershed/Vanilla'

# FIXME
# use os.path.join
inputs 	= ''
outputs	= ''
path_for_input = os.path.join(path, inputs)
path_for_output =  os.path.join(path, outputs)

read_from = int(sys.argv[1])
read_until = int(sys.argv[2])  


while read_from <= read_until:
	
	image = path_for_input + str(read_from) + '.jpg'
	
	img = io.imread(image)			    	        # read image as a 2D numpy array
	img_gray = rgb2gray(img)

	thresh = threshold_otsu(img_gray)		        # return threshold value based on on otsu's method

	if bright_backround:
		foreground_mask = img_gray <= thresh            # for bright backround
	else:
		foreground_mask = img_gray > thressh 	        # for dark backround


	# compute the Euclidean distance from every binary pixel to the nearest zero pixel 
	# and then find peaks in this distance map
	#
	distance = ndimage.distance_transform_edt(foreground_mask)

	# return a boolean array shaped like image, with peaks represented by True values
	localMax = peak_local_max(distance, indices=False, min_distance=30, labels=foreground_mask)

	# perform a connected component analysis on the local peaks using 8-connectivity 
	#
	markers = ndimage.label(localMax, structure=np.ones((3, 3)))[0]
	
	# apply the Watershed algorithm
	labels = watershed(-distance, markers, mask=foreground_mask)

	#print "In image {}, there are {} segments found!".format(image, len(np.unique(labels)) - 1)


	# loop over the unique labels returned by the Watershed algorithm
	# each label is a unique object

	for label in np.unique(labels):
	
		# if the label is zero, we are examining the 'background' so ignore it
		#
		if label != 0:

			mask = np.zeros(img_gray.shape, dtype="uint8")       # create a black mask	
			mask[labels == label] = 255	        # it make the pixels that correspond to the label-object white

			# compute the sobel transform of the mask to detect the label's-object's edges
			edge_sobel = sobel(mask)
			# make all the pixel, which correspond to label's edges, green in the image					
			img[edge_sobel > 0] = [0,255,0]

	io.imsave(path_for_output + 'out' + str(read_from) + '.jpg', img)

	read_from += 1

#-------------------------------------------------------------------------------
