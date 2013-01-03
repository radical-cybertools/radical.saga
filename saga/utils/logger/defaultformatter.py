
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides the default log output formatter for SAGA.
'''

from logging import Formatter

DefaultFormatter = Formatter(fmt='%(asctime)s %(name)s: [%(levelname)-8s] %(message)s', 
                             datefmt='%m/%d/%Y %I:%M:%S %p')

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

