import os
import argparse
import pprint
pp = pprint.PrettyPrinter().pprint


def dem_multi(path, from_image, until_image, imgext, inputs, outputs):
    """From the inputs and ouputs folder located in path, we retrieve images
    from inputs folder, run the dem algorithm on it, then save it
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

    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    from pydem.dem_processing import DEMProcessor
    from imageio import imwrite

    def dem_analyze(input_path, output_path):
        """Runs the dem algorithm on the image at input_path
        and saves it to output_path
        
        PARAMETERS
        -----------
        input_path : string
            absolute path to the input image

        output_path : string
            absolute path to the output image
        """
        
        dem_proc = DEMProcessor(input_path)

        mag, asp = dem_proc.calc_slopes_directions()

        #Calculate the upstream contributing area:
        uca = dem_proc.calc_uca()

        #Calculate the TWI:
        twi = dem_proc.calc_twi()

        imwrite(output_path % 'mag', mag)
        imwrite(output_path % 'asp', asp)
        imwrite(output_path % 'uca', uca)
        imwrite(output_path % 'twi', twi)

        return

    
    print 'Input Arguments'
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
        input_path = os.path.join(path_for_input, str(from_image) + imgext)

        # absolute output image path
        output_path = os.path.join(path_for_output, str(from_image) + '%s' + imgext)

        dem_analyze(input_path, output_path)
        
        print ' [x] saved to %s' % output_path

        from_image += 1

    return

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
imgext              = args.imgext
inputs              = args.inputs
outputs             = args.outputs

if args.verbosity >= 2:
    print('Input Arguments')
    pp([   ['path             ' , path              ],
           ['read_from        ' , read_from         ],
           ['read_until       ' , read_until        ],
           ['imgext           ' , imgext            ],
           ['inputs           ' , inputs            ],
           ['outputs          ' , outputs           ]
           
       ])
if args.verbosity >= 1:
    print 'Arguments are valid'

if __name__ == '__main__':
    dem_multi(path, read_from, read_until, imgext, inputs, outputs)