# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides API handles for SAGA's configuration system.
'''

import os
from saga.utils.logging import getLogger
from saga.utils.singleton import Singleton
from saga.utils.exception import ExceptionBase

## These are all supported options for saga.core
_all_core_options = [
{ 
  'category'      : 'saga.core.logging',
  'name'          : 'level', 
  'type'          : type(str), 
  'default'       : 'DEBUG', 
  'valid_options' : ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
  'documentation' : 'The log level',
  'env_variable'  : 'SAGA_VERBOSE' # or None
 },
 { 
  'category'      : 'saga.core.logging',
  'name'          : 'filters', 
  'type'          : type(list), 
  'default'       : None, 
  'valid_options' : None,
  'documentation' : 'The log filters',
  'env_variable'  : 'SAGA_LOG_FILTER' # or None
 },
 { 
  'category'      : 'saga.core.logging',
  'name'          : 'targets', 
  'type'          : type(list), 
  'default'       : ['STDOUT'], 
  'valid_options' : None,
  'documentation' : 'The log targets',
  'env_variable'  : 'SAGA_LOG_TARGETS' # or None
 },
 { 
  'category'      : 'saga.core.logging',
  'name'          : 'ttycolor', 
  'type'          : type(bool), 
  'default'       : True, 
  'valid_options' : None,
  'documentation' : 'Whether to use colors for console output or not.',
  'env_variable'  : 'SAGA_LOG_TARGETS' # or None
 },

]

class _ConfigOption(object):

    def __init__(self, category, name, val_type, default_val, valid_options,
                 documentation, env_var):
        self._category = category
        self._name = name
        self._val_type = val_type
        self._default_val = default_val
        self._valid_options = valid_options
        self._env_var = env_var
        self._documentation = documentation
        self._value = None

    def __str__(self):
        return self._value

    @property
    def category(self):
        return self._category

    def set_value(self, value):
        self._value = value

    def get_value(self):
        return self._value


class Config(object): 
    __metaclass__ = Singleton

    def __init__(self):

        self._master_config = dict()
        self._setup()

    def _setup(self):
        # load valid options and add them to the configuration
        for option in _all_core_options:
            cat = option['category']
            if cat not in self._master_config:
                # first occurrence - add new category key
                self._master_config[cat] = dict()

            # check if env variable is set for this option
            ev = os.environ.get(option['env_variable'])
            if ev is not None:
                value = ev
            else:
                value = option['default']

            self._master_config[cat][option['name']] = _ConfigOption(
                option['category'],
                option['name'],
                option['type'],
                option['default'],
                option['valid_options'],
                option['documentation'],
                option['env_variable'])
            self._master_config[cat][option['name']].set_value(value)

    def get_category(self, category_name):
        if category_name not in self._master_config:
            raise CategoryNotFound(category_name)
        else:
            return self._master_config[category_name]

def getConfig():
    ''' Returns a handle to logging system's configuration.
    '''
    return Config() 

def tests():
    # make sure singleton works
    assert getConfig() == getConfig()
    assert getConfig() == Config()

    c = getConfig()
    print c.get_category('saga.core.logging')

class CategoryNotFound(ExceptionBase):
    def __init__(self, name):
        self.message = "A category with name '%s' could not be found." % name
    

