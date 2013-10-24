
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Unit tests for saga.engine.config.py
"""

from saga.utils.config import *

import radical.utils as ru


############################# BEGIN UNIT TESTS ################################
##

class _TestConfigurable(Configurable):
    """ A mock-class to test the Configurable interface.
    """
    _valid_options = [
        { 
        'category'      : 'saga.test',
        'name'          : 'level', 
        'type'          : str, 
        'default'       : 'CRITICAL', 
        'valid_options' : ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        'documentation' : 'The log level',
        'env_variable'  : 'SAGA_VERBOSE'
        },
        { 
        'category'      : 'saga.test',
        'name'          : 'filters', 
        'type'          : list, 
        'default'       : [], 
        'documentation' : 'The log filters',
        'env_variable'  : 'SAGA_LOG_FILTER' 
       },
       { 
        'category'      : 'saga.test',
        'name'          : 'targets', 
        'type'          : list, 
        'default'       : ['STDOUT'], 
        'documentation' : 'The log targets',
        'env_variable'  : 'SAGA_LOG_TARGETS' 
       },
       { 
        'category'      : 'saga.test',
        'name'          : 'ttycolor', 
        'type'          : bool, 
        'default'       : True, 
        'documentation' : 'Whether to use colors for console output or not.',
        'env_variable'  : None 
       }]

    def __init__(self):
        Configurable.__init__(self, 'saga.test', _TestConfigurable._valid_options)

def test_singleton():
    """ Test that the object behaves like a singleton
    """
    # make sure singleton works
    assert(getConfig() == getConfig())
    assert(getConfig() == Configuration())

    _TestConfigurable()

    assert(getConfig().get_category('saga.test') == 
      Configuration().get_category('saga.test'))

    assert(getConfig().get_option('saga.test', 'level') == 
      Configuration().get_option('saga.test', 'level'))

def test_CategoryNotFound_exceptions():
    """ Test if CategoryNotFound exceptions are thrown as expected
    """
    try:
        getConfig().get_category('nonexistent')
        assert False
    except CategoryNotFound:
        assert True

def test_OptionNotFound_exceptions():
    """ Test if OptionNotFound exceptions are thrown as expected
    """
    try:
        getConfig().get_option('saga.test', 'nonexistent')
        assert False
    except OptionNotFound:
        assert True

def test_ValueTypeError_exception():
    """ Test if ValueTypeError exceptions are thrown as expected
    """
    try:
        # try to set wrong type
        getConfig().get_option('saga.test', 'ttycolor').set_value('yes')
        assert False
    except ValueTypeError:
        assert True

def test_get_set_value():
    """ Test if get/set value works as expected
    """
    getConfig().get_option('saga.test', 'ttycolor').set_value(False)
    assert(getConfig().get_option('saga.test', 'ttycolor').get_value()
      == False)
    
    getConfig().get_option('saga.test', 'ttycolor').set_value(True)
    assert(getConfig().get_option('saga.test', 'ttycolor').get_value()
      == True)

def test_env_vars():
    """ Test if environment variables are handled properly
    """
    # for this test, we call the private _initialize() method again to make
    # sure Configuration reads the environment variables again.

    # save SAGA_VERBOSE state
    tmp_ev = os.environ.get('SAGA_VERBOSE')

    os.environ['SAGA_VERBOSE'] = 'INFO'
    cfg = getConfig()
    cfg._initialize()

    assert(getConfig().get_option('saga.test', 'level').get_value()
      == 'INFO')

    # reset SAGA_VERBOSE
    if tmp_ev is not None:
        os.environ['SAGA_VERBOSE'] = tmp_ev

def test_valid_config_file():
    """ Test if a valid config file can be parsed properly
    """
    # Generate a configuration file
    import tempfile
    import ConfigParser

    tmpfile = open('/tmp/saga.conf', 'w+')
    config = ConfigParser.RawConfigParser()

    config.add_section('saga.test')
    config.set('saga.test', 'ttycolor', False)
    config.set('saga.test', 'filters', 'saga,saga.adaptor.pbs')
    config.write(tmpfile)
    tmpfile.close()

    # for this test, we call the private _initialize() method again to read
    # an alternative (generated) config file.
    cfg = getConfig()
    cfg._initialize(tmpfile.name)

    # make sure values appear in Configuration as set in the config file
    assert(getConfig().get_option('saga.test', 'filters').get_value()
      == ['saga', 'saga.adaptor.pbs'])

    # make sure a signgle-element list works as well

    tmpfile = open('/tmp/saga.conf', 'w+')
    config = ConfigParser.RawConfigParser()

    config.add_section('saga.test')
    config.set('saga.test', 'ttycolor', False)
    config.set('saga.test', 'filters', 'justonefilter')
    config.write(tmpfile)
    tmpfile.close()

    # for this test, we call the private _initialize() method again to read
    # an alternative (generated) config file.
    cfg = getConfig()
    cfg._initialize(tmpfile.name)

    # make sure values appear in Configuration as set in the config file
    assert(getConfig().get_option('saga.test', 'filters').get_value()
      == ['justonefilter'])

    # make sure a zero-elemnt list works as well
    tmpfile = open('/tmp/saga.conf', 'w+')
    
    config = ConfigParser.RawConfigParser()

    config.add_section('saga.test')
    config.set('saga.test', 'ttycolor', False)
    config.write(tmpfile)
    tmpfile.close()

    # for this test, we call the private _initialize() method again to read
    # an alternative (generated) config file.
    cfg = getConfig()
    cfg._initialize(tmpfile.name)

    # make sure values appear in Configuration as set in the config file
    assert(getConfig().get_option('saga.test', 'filters').get_value()
      == [])
    assert(getConfig().get_option('saga.test', 'ttycolor').get_value()
      == False)
        

def test_invalid_config_file():
    """ Test if an invalid config file is handled properly
    """
    import tempfile
    import ConfigParser

    tmpfile = open('/tmp/saga.conf', 'w+')
    config = ConfigParser.RawConfigParser()

    config.add_section('saga.test')
    config.set('saga.test', 'ttycolor', 'invalid')
    config.write(tmpfile)
    tmpfile.close()

    # for this test, we call the private _initialize() method again to read
    # an alternative (generated) config file.
    try: 
        cfg = getConfig()
        cfg._initialize(tmpfile.name)
        assert False
    except ValueTypeError:
        assert True



