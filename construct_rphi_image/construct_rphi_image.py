import matplotlib.pyplot as plt
import numpy as np
from skbeam.core.image import construct_rphi_avg_image

## Generate the image

from skbeam.core.utils import angle_grid, radial_grid
# first generate some random scattering pattern
# There are missing regions

shape = 800,800
x0,y0 = 401, 401
ANGLES = angle_grid((y0, x0), shape)
RADII = radial_grid((y0, x0), shape)
img = np.cos(ANGLES*5)**2*RADII**2

mask = np.ones_like((ANGLES))
mask[100:200] = 0
mask[:,100:200] = 0
mask[:,500:643] = 0
mask[70:130] = 0

img*=mask



# reconstruct the image into polar grid

from skbeam.core.accumulators.binned_statistic import RPhiBinnedStatistic
rphibinstat = RPhiBinnedStatistic(shape, bins=(400,360), mask=mask, origin=(y0,x0))
rphi_img = rphibinstat(img)
# create mask from np.nans since RPhiBinnedStatistic fills masked regions with np.nans
rphimask = np.ones_like(rphi_img)
rphimask[np.where(np.isnan(rphi_img))] = 0



