# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides API handles for SAGA's core runtime.
'''

from saga.utils.singleton import Singleton
from saga.core.config import Configurable, Configuration, getConfig
from saga.core.logger import Logger, getLogger

############# These are all supported options for saga.core ####################
##
_all_core_config_options = [
    { 
    'category'      : 'saga.core',
    'name'          : 'foo', 
    'type'          : str, 
    'default'       : 'bar', 
    'valid_options' : None,
    'documentation' : 'A sample configuration option. Does nothing.',
    'env_variable'  : None
    }
]

################################################################################
##
class Core(Configurable): 
    ''' Represents the SAGA core runtime system.

        The Core class is a singleton class that takes care of 
        configuration, logging and adaptor management. Core is 
        instantiated implicitly as soon as SAGA is imported into
        Python. It can be used to introspect the current state of
        a SAGA instance.
    '''
    __metaclass__ = Singleton

    def __init__(self):
        # set the configuration options for this object
        Configurable.__init__(self, 'saga.core', _all_core_config_options)
        # initialize logging
        self._initializeLogging()

    def _initializeLogging(self):
        Logger()

    def _loadAdaptors(self):
        pass

    def listLoadedAdaptors(self):
        pass

def getCore():
    """ Returns a handle to the Core object.
    """
    return Core() 

############################# BEGIN UNIT TESTS ################################
##
def test_singleton():
    # make sure singleton works
    assert(getCore() == getCore())
    assert(getCore() == Core())

def test_configurable():
    # make sure singleton works
    assert Core().get_config()['foo'].get_value() == 'bar'
    
##
############################## END UNIT TESTS #################################
