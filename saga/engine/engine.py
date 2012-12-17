# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides API handles for SAGA's runtime.
'''

from saga.utils.singleton import Singleton
from saga.engine.config   import Configurable, Configuration, getConfig
from saga.engine.logger   import Logger, getLogger
from saga.engine.registry import adaptor_registry

import os
import re
import sys
import pprint
import string
import inspect
import collections

import saga

############# These are all supported options for saga.engine ####################
##
_all_engine_config_options = [
    { 
    'category'      : 'saga.engine',
    'name'          : 'foo', 
    'type'          : str, 
    'default'       : 'bar', 
    'valid_options' : None,
    'documentation' : 'dummy config option for unit test.',
    'env_variable'  : None
    },
    { 
    'category'      : 'saga.engine',
    'name'          : 'adaptor_paths', 
    'type'          : str, 
    'default'       : None, 
    'valid_options' : None,
    'documentation' : 'additional adaptor search paths.',
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
              
              for adaptor_class in self._engine._adaptors{'job'}{'http'}
                  try :
                      self._adaptor = adaptor_class (url, session}
                  except saga.Exception e :
                      # adaptor could not handle the URL, handle e
                  else :
                      # successfully bound to adaptor
                      return

        Adaptors to be loaded are searched for, by default in the module's
        'adaptors/' subdirectory.  The config option 'adaptor_path' can 
        specify additional (comma separated) paths to search.  The engine 
        will attempt to load any python module named 'saga_adaptor_[name].py'
        in any of the specified paths (default path first, then configured 
        paths in the specified order).
        '
    '''
    __metaclass__ = Singleton

    def __init__(self):
        
        # Engine manages adaptors
        self._adaptors = collections.defaultdict (dict)

        # set the configuration options for this object
        Configurable.__init__(self, 'saga.engine', _all_engine_config_options)

        # initialize logging
        self._initialize_logging()

        # load adaptors
        self._load_adaptors()


    def _initialize_logging(self):
        Logger()


    def _load_adaptors(self):

        # global_conf = saga.engine.getConfig()

        for module_name in adaptor_registry :

            print "engine: load adaptor  : " + module_name

            try :
                adaptor_module = __import__ (module_name, fromlist=['register'])

                # we expect the module to have an 'register' method implemented,
                # which returns a info dict for all implemented CPI classes
                adaptor_infos = adaptor_module.register ()

                pprint.pprint (adaptor_infos)

                for adaptor_info in adaptor_infos :
                    adaptor_name    = adaptor_info['name']
                    adaptor_type    = adaptor_info['type']
                    adaptor_class   = adaptor_info['class']
                    adaptor_schemas = adaptor_info['schemas']

                    # check if adaptor is disabled via config
                    disabled = self.get_config()['foo'].get_value() == 'bar'  

                    for adaptor_schema in adaptor_schemas :

                        adp_class = getattr (adaptor_module, adaptor_class)
                        self._adaptors[adaptor_type][adaptor_schema] = adp_class

                pprint.pprint (dict(self._adaptors))

            except Exception as e :
                print "engine: loading failed: " + module_name
                print str (e)


    def list_loaded_adaptors(self):

        pprint.pprint (dict(self._adaptors))

        pass


############################# BEGIN UNIT TESTS ################################
##
def test_singleton():
    # make sure singleton works
    assert(getEngine() == getEngine())
    assert(getEngine() == Engine())

    e1 = Engine()
    e2 = Engine()
    assert(e1 == e2)

def test_configurable():
    # make sure singleton works
    assert Engine().get_config()['foo'].get_value() == 'bar'  
##
############################## END UNIT TESTS #################################

