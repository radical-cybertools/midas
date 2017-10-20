import numpy as np

from skbeam.core.image import construct_rphi_avg_image
from skbeam.core.utils import angle_grid, radial_grid

#######################################################
#CONFIGURATIONS

shape = 800, 800    #size of the image



####################################################






ANGLES = angle_grid((y0, x0), shape)
RADII = radial_grid((y0, x0), shape)
img = np.cos(ANGLES*5)**2*RADII**2

mask = np.ones_like((ANGLES))
mask[100:200] = 0
mask[:,100:200] = 0
mask[:,500:643] = 0
mask[70:130] = 0

img*=mask




#### stream the img
#### decide how to stream the img
### make sure that I calculate the correct size of the image
