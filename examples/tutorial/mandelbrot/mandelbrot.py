
__author__    = "Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" A Simple Mandelbrot Fractal Generator.

    We use this example to explore the distributed capabilites of
    the SAGA Job and Filesystem APIs. The mandelbrot module
    calculates a full or partial (tile) mandelbrot set fractal and
    writes it to a PNG image file.

    It requires the Python Image Library (PIL) which can be easily
    installed with 'easy_install PIL'.

    The mandelbrot module can be called either as a function::

        from mandelbrot import makemandel
        makemandel( imgX, imgY, xBeg, xEnd, yBeg, yEnd, filename)

    or alternatively on the command line::

        python mandelbrot.py imgX imgY xBeg xEnd yBeg yEnd filename

    The parameters are as follows:

        imgX, imgY: the dimensions of the mandelbrot image, e.g. 1024, 1024
        xBeg, xEnd: the x-axis portion of the (sub-)image to calculate
        yBeg, yEnd: the y-axis portion of the (sub-)image to calculate
        filename: the output filename (defaults to mandel_x_%s_%s_y%s_%s.png)
"""

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

import sys, Image

################################################################################
##
def makemandel(mandelx, mandely, xbeg, xend, ybeg, yend, filename=None):

    # drawing area (xa < xb and ya < yb)
    xa = -2.0
    xb =  1.0
    ya = -1.5
    yb =  1.5

    # maximum iterations
    maxIt = 128 

    # the output image
    image = Image.new("RGB", (xend-xbeg, yend-ybeg))

    for y in range(ybeg, yend):
        cy = y * (yb - ya) / (mandely - 1)  + ya
        for x in range(xbeg, xend):
            cx = x * (xb - xa) / (mandelx - 1) + xa
            c = complex(cx, cy)
            z = 0
            for i in range(maxIt):
                if abs(z) > 2.0: break 
                z = z * z + c 
            r = i % 4 * 16
            g = i % 6 * 16
            b = i % 16 * 16
            image.putpixel((x-xbeg, y-ybeg), b * 65536 + g * 256 + r)
 
    if filename is not None:
        image.save(filename, "GIF")
    else:
        image.save("mandel_x_%s_%s_y%s_%s.gif" % (xbeg, xend, ybeg, yend), "GIF")
    return image

################################################################################
##
if __name__ == "__main__":

    args = sys.argv[1:]
    if len(args) < 6:
        print("Usage: python %s imgX imgY xBeg xEnd yBeg yEnd filename" % __file__)
        sys.exit(-1)

    imgX = int(sys.argv[1])
    imgY = int(sys.argv[2])
    xBeg = int(sys.argv[3])
    xEnd = int(sys.argv[4])
    yBeg = int(sys.argv[5])
    yEnd = int(sys.argv[6])
   
    filename = None 
    if len(args) == 7:
        filename = str(sys.argv[7])
    
    makemandel(imgX, imgY, xBeg, xEnd, yBeg, yEnd, filename)
    sys.exit(0)
