# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides log handler management for SAGA.
'''

import os
from logging import StreamHandler, Filter, getLogger as logging_getLogger
from logging import INFO, DEBUG, ERROR, WARNING, CRITICAL
from saga.utils.logger.colorstreamhandler import *
from saga.utils.logger.filehandler import FileHandler
from saga.utils.logger.defaultformatter import DefaultFormatter
from saga.utils.singleton import Singleton
from saga.utils.exception import ExceptionBase

from saga.core.config import Configurable

############# These are all supported options for saga.logging #################
##
_all_logging_options = [
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

################################################################################
##
class Logger(Configurable):
    __metaclass__ = Singleton

    def __init__(self):
        
        Configurable.__init__(self, 'saga.core.logging', _all_logging_options)    
        cfg = self.get_config()

        self._loglevel = cfg['level'].get_value()
        self._filters  = cfg['filters'].get_value()
        self._targets  = cfg['targets'].get_value()

        SAGA_VERBOSE = self._loglevel

        if SAGA_VERBOSE is not None:
            if SAGA_VERBOSE.isdigit():
                SAGA_VERBOSE = int(SAGA_VERBOSE)
                # 4 = DEBUG + INFO + WARNING + ERROR
                if SAGA_VERBOSE >= 4:
                    self._loglevel = DEBUG
                # 3 = INFO + WARNING + ERROR
                elif SAGA_VERBOSE == 3:
                    self._loglevel = INFO
                # 2 = WARNING + ERROR 
                elif SAGA_VERBOSE == 2:
                    self._loglevel = WARNING
                # 1 = ERROR ONLY
                elif SAGA_VERBOSE == 1:
                    self._loglevel = ERROR
                # 0 = No Logging
                elif SAGA_VERBOSE == 0:
                    self._loglevel = CRITICAL
                else:
                    raise LoggingException('%s is not a valid value for SAGA_VERBOSE.' % SAGA_VERBOSE)
            else:
                SAGA_VERBOSE_lower = SAGA_VERBOSE.lower()
                # 4 = DEBUG + INFO + WARNING + ERROR
                if SAGA_VERBOSE_lower == 'debug':
                    self._loglevel = DEBUG
                # 3 = INFO + WARNING + ERROR
                elif SAGA_VERBOSE_lower == 'info':
                    self._loglevel = INFO
                # 2 = WARNING + ERROR 
                elif SAGA_VERBOSE_lower == 'warning':
                    self._loglevel = WARNING
                # 1 = ERROR ONLY
                elif SAGA_VERBOSE_lower == 'error':
                    self._loglevel = ERROR
                # 0 = No Logging
                elif SAGA_VERBOSE_lower == 'critical':
                    self._loglevel = CRITICAL
                else:
                    raise LoggingException('%s is not a valid value for SAGA_VERBOSE.' % SAGA_VERBOSE)

    @property
    def loglevel(self):
        return self._loglevel

    @property
    def filters(self):
        return self._filters

    @property
    def targets(self):
        return self._targets


################################################################################
##
def getLogger(module, obj=None):
    ''' Get the SAGA logger.
    '''
    class _MultiNameFilter(Filter):
        def __init__(self, names):
            self._names = names
        def filter(self, record):
            for n in self._names:
                if n in record.name:
                    return True

    if obj is None:
        _logger = logging_getLogger('saga.%s' % module)
    else:
        _logger = logging_getLogger('saga.%s.%s' % (module, obj))

    # check how many handlers we need
    for target in Logger().targets:
        if target.lower() == 'stdout':
            # create a console stream logger
            # Only enable colour if support was loaded properly
            if has_color_stream_handler is True:
                handler = ColorStreamHandler()
            else: 
                handler = StreamHandler()
        else:
            # got to be a file logger
            handler = FileHandler(target)

        handler.setFormatter(DefaultFormatter)
        if Logger().filters is not None:
            handler.addFilter(_MultiNameFilter(Logger().filters))

        _logger.addHandler(handler)

    _logger.setLevel(Logger().loglevel)
    _logger.propagate = 0 # Don't bubble up to the root logger

    return _logger


################################################################################
##
class LoggingException(ExceptionBase):
    pass


############################# BEGIN UNIT TESTS ################################
##
def test_singleton():
    # make sure singleton works
    #assert(getLogger() == getLogger())
    assert Logger() == Logger() 
    assert getLogger('core') == getLogger('core')

def test_configurable():
    # make sure singleton works
    c = Logger().get_config()
    
    assert c['ttycolor'].get_value() == True
    assert c['filters'].get_value() == []
    #assert c['level'].get_value() == 'CRITICAL'
    assert c['targets'].get_value() == ['STDOUT']

def test_logger():
    cl = getLogger('core')
    assert cl is not None
    cl.debug('debug')
    cl.info('info')
    cl.warning('warning')
    cl.error('error')
    cl.fatal('fatal')
##
############################## END UNIT TESTS #################################
