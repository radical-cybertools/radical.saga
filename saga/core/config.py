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
from saga.utils.configfile import ConfigFileReader


########### These are all supported options for saga.core #####################
##
_all_core_options = [
{ 
  'category'      : 'saga.core.logging',
  'name'          : 'level', 
  'type'          : str, 
  'default'       : 'CRITICAL', 
  'valid_options' : ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
  'documentation' : 'The log level',
  'env_variable'  : 'SAGA_VERBOSE'
 },
 { 
  'category'      : 'saga.core.logging',
  'name'          : 'filters', 
  'type'          : list, 
  'default'       : [], 
  'valid_options' : None,
  'documentation' : 'The log filters',
  'env_variable'  : 'SAGA_LOG_FILTER' 
 },
 { 
  'category'      : 'saga.core.logging',
  'name'          : 'targets', 
  'type'          : list, 
  'default'       : ['STDOUT'], 
  'valid_options' : None,
  'documentation' : 'The log targets',
  'env_variable'  : 'SAGA_LOG_TARGETS' 
 },
 { 
  'category'      : 'saga.core.logging',
  'name'          : 'ttycolor', 
  'type'          : bool, 
  'default'       : True, 
  'valid_options' : None,
  'documentation' : 'Whether to use colors for console output or not.',
  'env_variable'  : None 
 },
]
##
###############################################################################

class ConfigOption(object):
    """ Represent a (mutable) configuration option.
    """

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
        return str({'name':self._name, 'value':self._value})

    @property
    def category(self):
        return self._category

    def set_value(self, value):
        # make sure we got the right value type
        if type(value) != self._val_type:
            raise ValueTypeError(self._category, self._name, 
              type(value), self._val_type)

        self._value = value

    def get_value(self):
        return self._value


class GlobalConfig(object): 
    """ Represents SAGA's global configuration.

        The GlobalConfig class can be used to introspect and modify the
        configuration options for SAGA and its various middleware adaptors. 
        It is a 'Singleton' object, which means that  multiple instances all 
        point to the same object which holds the global configuration.
    """    
    __metaclass__ = Singleton

    def __init__(self):

        self._master_config = dict()
        self._initialize()

    def _initialize(self, inject_cfg_file=None):
        # inject_cfg_file is used *only* for testing purposes and overwrites /
        # ignores the regular config file locations /etc/saga.cfg & $HOME/.saga.cfg
        cfg_files = list()
        if inject_cfg_file is not None:
            cfg_files.append(inject_cfg_file)
        else:
            # check for the existence of regular configuration files
            sys_cfg = '/etc/saga.cfg'
            if os.path.exists(sys_cfg):
                cfg_files.append(sys_cfg)
            usr_cfg = '%s/.saga.cfg' % os.path.expanduser("~")
            if os.path.exists(usr_cfg):
                cfg_files.append(usr_cfg)

        cfr = ConfigFileReader(cfg_files)

        # load valid options and add them to the configuration
        for option in _all_core_options:
            cat = option['category']
            if cat not in self._master_config:
                # first occurrence - add new category key
                self._master_config[cat] = dict()

            # check if env variable is set for this option
            ev = os.environ.get(option['env_variable'])
            if ev is not None:
                getLogger('core').debug("Using environment variable '%s' to set config option '%s.%s' to '%s'." \
                    % (option['env_variable'], option['category'], option['name'], ev))
                value = ev
            else:
                value = option['default']

            self._master_config[cat][option['name']] = ConfigOption(
                option['category'],
                option['name'],
                option['type'],
                option['default'],
                option['valid_options'],
                option['documentation'],
                option['env_variable'])
            self._master_config[cat][option['name']].set_value(value) 

    def get_category(self, category_name):
        """ Return a specific configuration category.
        """
        if category_name not in self._master_config:
            raise CategoryNotFound(category_name)
        else:
            return self._master_config[category_name]

    def get_option(self, category_name, option_name):
        if category_name not in self._master_config:
            raise CategoryNotFound(category_name)
        else:
            if option_name not in self._master_config[category_name]:
                raise OptionNotFound(category_name, option_name)
            else:
                return self._master_config[category_name][option_name]

def getConfig():
    """ Returns a handle to logging system's configuration.
    """
    return GlobalConfig() 


class CategoryNotFound(ExceptionBase):
    def __init__(self, name):
        self.message = "A category with name '%s' could not be found." % name


class OptionNotFound(ExceptionBase):
    def __init__(self, category_name, option_name):
        name = "%s.%s" % (category_name, option_name)
        self.message = "An option with name '%s' could not be found." % (name)

class ValueTypeError(ExceptionBase):
    def __init__(self, category_name, option_name, value_type, required_type):
        name = "%s.%s" % (category_name, option_name)
        self.message = "Option %s requires value of type '%s' but got '%s'." % \
            (name, required_type, value_type)


############################# BEGIN UNIT TESTS ################################
##
def test_singleton():
    # make sure singleton works
    assert(getConfig() == getConfig())
    assert(getConfig() == GlobalConfig())

    assert(getConfig().get_category('saga.core.logging') == 
      GlobalConfig().get_category('saga.core.logging'))

    assert(getConfig().get_option('saga.core.logging', 'level') == 
      GlobalConfig().get_option('saga.core.logging', 'level'))

def test_CategoryNotFound_exceptions():
    try:
        getConfig().get_category('nonexistent')
        assert False
    except CategoryNotFound:
        assert True

def test_OptionNotFound_exceptions():
    try:
        getConfig().get_option('saga.core.logging', 'nonexistent')
        assert False
    except OptionNotFound:
        assert True

def test_get_set_value():
    getConfig().get_option('saga.core.logging', 'ttycolor').set_value(False)
    assert(getConfig().get_option('saga.core.logging', 'ttycolor').get_value()
      == False)
    
    getConfig().get_option('saga.core.logging', 'ttycolor').set_value(True)
    assert(getConfig().get_option('saga.core.logging', 'ttycolor').get_value()
      == True)

def test_ValueTypeError_exception():
    try:
        # try to set wrong type
        getConfig().get_option('saga.core.logging', 'ttycolor').set_value('yes')
        assert False
    except ValueTypeError:
        assert True

def test_env_vars():
    # for this test, we call the private _initialize() method again to make
    # sure GlobalConfig() reads the environment variables again.
     os.environ['SAGA_VERBOSE'] = 'INFO'
     cfg = getConfig()
     cfg._initialize()

     assert(getConfig().get_option('saga.core.logging', 'level').get_value()
       == 'INFO')

def test_valid_config_file():
    # Generate a configuration file
    import tempfile
    tmpfile = open('/tmp/saga.conf', 'w+')
    
    import ConfigParser
    config = ConfigParser.RawConfigParser()

    config.add_section('saga.core.logging')
    config.set('saga.core.logging', 'ttycolor', False)
    config.set('saga.core.logging', 'filters', ['saga', 'saga.adaptor.pbs'])
    config.write(tmpfile)
    tmpfile.close()

    # for this test, we call the private _initialize() method again to read
    # an alternative (generated) config file.
    cfg = getConfig()
    cfg._initialize(tmpfile.name)

    # make sure values appear in GlobalConfig as set in the config file
    print getConfig().get_option('saga.core.logging', 'filters').get_value()
    assert(getConfig().get_option('saga.core.logging', 'filters').get_value()
      == ['saga', 'saga.adaptor.pbs'])

    

def test_invalid_config_file():
  pass
##
############################## END UNIT TESTS #################################
