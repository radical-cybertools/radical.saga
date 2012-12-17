# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides API handles for SAGA's runtime.
'''

from saga.utils.singleton import Singleton
from saga.engine.config import Configurable, Configuration, getConfig
from saga.engine.logger import Logger, getLogger

############# These are all supported options for saga.engine ####################
##
_all_engine_config_options = [
    { 
    'category'      : 'saga.engine',
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
def getEngine():
    """ Returns a handle to the Engine object.
    """
    return Engine() 


################################################################################
##
class Engine(Configurable): 
    ''' Represents the SAGA engine runtime system.

        The Engine class is a singleton class that takes care of 
        configuration, logging and adaptor management. Engine is 
        instantiated implicitly as soon as SAGA is imported into
        Python. It can be used to introspect the current state of
        a SAGA instance.

        While loading adaptors, the Engine builds up a registry of 
        adaptor classes, hierarchically sorted like this::

          _adaptors = 
          { 
              'job' : 
              { 
                  'gram' : [<gram job  adaptor class>]
                  'ssh'  : [<ssh  job  adaptor class>]
                  'http' : [<aws  job  adaptor class>,
                            <occi job  adaptor class>]
                  ...
              },
              'file' : 
              { 
                  'ftp'  : <ftp  file adaptor class>
                  'scp'  : <scp  file adaptor class>
                  ...
              },
              ...
          }

        to enable simple lookup operations when binding an API object to an
        adaptor class instance.  For example, a 
        'saga.job.Service('http://remote.host.net/')' constructor would use
        (simplified)::

          def __init__ (self, url, session=None) :
              
              for adaptor_class in self._engine._adaptors {'job'}{'http'}
                  try :
                      self._adaptor = adaptor_class (url, session}
                  except saga.Exception e :
                      # adaptor could not handle the URL, handle e
                  else :
                      # successfully bound to adaptor
                      return
        '
    '''
    __metaclass__ = Singleton

    def __init__(self):
        # set the configuration options for this object
        Configurable.__init__(self, 'saga.engine', _all_engine_config_options)
        # initialize logging
        self._initialize_logging()
        self._load_adaptors()

    def _initialize_logging(self):
        Logger()

    def _load_adaptors(self):
        pass

    def list_loaded_adaptors(self):
        pass


############################# BEGIN UNIT TESTS ################################
##
def test_singleton():
    # make sure singleton works
    assert(getEngine() == getEngine())
    assert(getEngine() == Engine())

def test_configurable():
    # make sure singleton works
    assert Engine().get_config()['foo'].get_value() == 'bar'  
##
############################## END UNIT TESTS #################################

