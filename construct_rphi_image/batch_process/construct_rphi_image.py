import matplotlib.pyplot as plt
import numpy as np
from skbeam.core.accumulators.binned_statistic import RPhiBinnedStatistic
from skbeam.core.utils import angle_grid, radial_grid
from skbeam.core.image import construct_rphi_avg_image
import os
import datetime
import time


run_timestamp=datetime.datetime.now()
RESULT_FILE= "results/batch-construct_phi-" + run_timestamp.strftime("%Y%m%d-%H%M%S") + ".csv"

try:
    os.makedirs('results')
except:
    pass

output_file = open(RESULT_FILE, "a")
output_file.write("Image_Dimensions,Image_size(MB),Iteration,TTC")

start = time.time()

## Generate the image
# first generate some random scattering pattern
# There are missing regions

pixel_sizes = [400,800,1600,3200]
pixel_sizes = [400]
for pixels in pixel_sizes:

    shape = pixel_sizes, pixel_sizes
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

    print (img.nbytes/1024)/1024  , 'MB'

    # reconstruct the image into polar grid
    ttr = time.time()
    rphibinstat = RPhiBinnedStatistic(shape, bins=(400,360), mask=mask, origin=(y0,x0))
    rphi_img = rphibinstat(img)
    # create mask from np.nans since RPhiBinnedStatistic fills masked regions with np.nans
    rphimask = np.ones_like(rphi_img)
    rphimask[np.where(np.isnan(rphi_img))] = 0
    ttrpolar = time.time() - ttr

    ## Regenerate the image to thes the construct_rphi_image

    # get angles and radii from (q, phi) polar coordinate system
    angles = rphibinstat.bin_centers[1]
    radii = rphibinstat.bin_centers[0]

    # reproject
    Zproj = construct_rphi_avg_image(radii, angles, rphi_img, shape=(800,800))


    # 10  fold symmetry
    # Let's add the same image but before reconstructing, shift phi by 2pi/10....

    sym = int(10)
    polar_shape = 500, 360 
    origin = x0, y0

def reconstruct_nfold(img, sym, polar_shape, mask=None, origin=None):
        ''' 
        Reconstruct an image assuming a certain symmetry.
            
        Parameters
        ----------
        img : the image
                
        sym : the symmetry of the sample
                
        polar_shape : the shape of the new polar coordinate image
            
        Returns
        -------
        reconstructed_image : the reconstructed  image
            
        '''
    shape = img.shape

    rphibinstat = RPhiBinnedStatistic(shape, bins=polar_shape, mask=mask, origin=origin)
    angles = rphibinstat.bin_centers[1]
    radii = rphibinstat.bin_centers[0]
    rphi_img = rphibinstat(img)
    # create mask from np.nans since RPhiBinnedStatistic fills masked regions with np.nans
    rphimask = np.ones_like(rphi_img)
    rphimask[np.where(np.isnan(rphi_img))] = 0

    reconstructed_image = np.zeros_like(img)
    reconstructed_image_mask = np.zeros_like(img,dtype=int)
    # symmetry
    dphi = 2*np.pi/float(sym)
    for i in range(sym):
        anglesp = angles + dphi*i
        imgtmp = construct_rphi_avg_image(radii, anglesp, rphi_img,
                                              shape=shape, center=origin, mask=rphimask)
        w = np.where(~np.isnan(imgtmp))
        reconstructed_image[w] += imgtmp[w]
        reconstructed_image_mask += (~np.isnan(imgtmp)).astype(int)
            
        # the mask keeps count of included pixels. Average by this amount
    reconstructed_image /= reconstructed_image_mask
    
    return reconstructed_image

    reconstructed_image = reconstruct_nfold(img, sym, polar_shape, mask=mask, origin=origin)

    end = time.time() - start

    print 'Total time is: ', end , ' seconds
