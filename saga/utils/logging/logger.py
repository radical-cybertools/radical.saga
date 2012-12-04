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
from saga.utils.singleton import Singleton
from saga.utils.logging.colorstreamhandler import *
from saga.utils.logging.defaultformatter import DefaultFormatter

class Config(object):
    __metaclass__ = Singleton

    def __init__(self):
        
        # the default log level
        self._loglevel = CRITICAL
        self._filters = None

        SAGA_VERBOSE = os.environ.get('SAGA_VERBOSE')

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
                else:
                    self._loglevel = CRITICAL
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
                else:
                    self._loglevel = CRITICAL

        SAGA_LOG_FILTER = os.environ.get('SAGA_LOG_FILTER')
        if SAGA_LOG_FILTER is not None:
            print SAGA_LOG_FILTER
            try:
                self._filters = list()
                for x in SAGA_LOG_FILTER.split(','):
                    self._filters.append(x)
            except Exception, ex:
                raise LoggingException('%s is not a valid value for SAGA_LOG_FILTER.' % SAGA_LOG_FILTER)

        saga_log_target = os.environ.get('SAGA_LOG_TARGET')
        if saga_log_target is not None:
            pass
        else:
            self._target = 'STDOUT'

    @property
    def loglevel(self):
        return self._loglevel

    @property
    def filters(self):
        return self._filters

    @property
    def target(self):
        return self._target

class LoggingException(Exception):
    pass

class _MultiNameFilter(Filter):

    def __init__(self, names):
        self._names = names

    def filter(self, record):
            if record.name in self._names:
                return True
            else:
                return False

def getLogger(module, obj=None):
    ''' Get the SAGA logger.
    '''
    if obj is None:
        _logger = logging_getLogger('saga.%s' % module)
    else:
        _logger = logging_getLogger('saga.%s.%s' % (module, obj))
    # Only enable colour if support was loaded properly
    if has_color_stream_handler is True:
        handler = ColorStreamHandler()
    else: 
        handler = StreamHandler()

    if Config().filters is not None:
        handler.addFilter(_MultiNameFilter(Config().filters))

    handler.setFormatter(DefaultFormatter)
    _logger.addHandler(handler)
    _logger.setLevel(Config().loglevel)
    _logger.propagate = 0 # Don't bubble up to the root logger

    return _logger

def _test_():

    lc = Config() 
    #lc.set_level(DEBUG) 
    #lc.set_target('/tmp/log.out')#saga.utils.logger.Config()

    l1 = getLogger('core')
    l1.debug('debug')
    l1.info('info')
    l1.warning('warning')
    l1.error('error')
    l1.fatal('fatal')

    l2 = getLogger('adaptor', 'pbs')    
    l2.debug('debug')
    l2.info('info')
    l2.warning('warning')
    l2.error('error')
    l2.fatal('fatal')



