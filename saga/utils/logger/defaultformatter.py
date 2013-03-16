
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


''' Provides the default log output formatter for SAGA.
'''

from logging import Formatter

# DefaultFormatter = Formatter(fmt=' %(funcName)-15s:%(module)-15s %(lineno)4d %(asctime)s %(name)-23s: [%(levelname)-8s] %(message)s', 
DefaultFormatter = Formatter(fmt='%(asctime)s %(thread)d %(name)-22s: [%(levelname)-8s] %(message)s', 
                             datefmt='%Y:%m:%d %H:%M:%S')

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

