
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides a file handler for the SAGA logging system.
'''

from logging import DEBUG, getLogger, Filter, FileHandler as LFileHandler
 
class FileHandler(LFileHandler):
    """ A output FileHandler. """
    pass

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

