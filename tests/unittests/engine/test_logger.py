
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


''' Unit tests for saga.engine.logger.py
'''

from saga.utils.logger        import getLogger
from saga.utils.logger.logger import _Logger

import radical.utils as ru


############################# BEGIN UNIT TESTS ################################
##
def test_singleton():
    """ Test if the logger behaves like a singleton
    """
    # make sure singleton works
    assert getLogger()         == getLogger()
    assert getLogger('engine') == getLogger('engine')

def test_configurable():
    """ Test if the logger config options work
    """
    # make sure singleton works
    c = _Logger().get_config()
    
    assert c['ttycolor'].get_value() == True
    assert c['filters'].get_value() == []
    #assert c['level'].get_value() == 'CRITICAL'
    assert c['targets'].get_value() == ['STDOUT']

def test_logger():
    """ Print out some messages with different log levels
    """
    cl = getLogger('engine')
    cl = getLogger('engine')
    
    assert cl is not None
    cl.debug('debug')
    cl.info('info')
    cl.warning('warning')
    cl.error('error')
    cl.fatal('fatal')



