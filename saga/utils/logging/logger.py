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

        self._loglevel = CRITICAL
        SAGA_VERBOSE = os.environ.get('SAGA_VERBOSE')
        if SAGA_VERBOSE is not None:
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

        #saga_log_level = os.environ.get('SAGA_LOG_LEVEL')
        #if saga_log_level is not None:
        #    pass
        #else:
        #    self._loglevel = DEBUG

        saga_log_filter = os.environ.get('SAGA_LOG_FILTER')
        if saga_log_filter is not None:
            pass
        else:
            self._filter = None

        saga_log_target = os.environ.get('SAGA_LOG_TARGET')
        if saga_log_target is not None:
            pass
        else:
            self._target = 'STDOUT'

    @property
    def loglevel(self):
        return self._loglevel

    @property
    def filter(self):
        return self._filter

    @property
    def target(self):
        return self._target

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

