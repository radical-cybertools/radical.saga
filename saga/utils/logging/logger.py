# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides log handler management for SAGA.
'''

import os
from logging import StreamHandler, Filter, getLogger as logging_getLogger
from logging import INFO, DEBUG, ERROR, WARNING
from saga.utils.singleton import Singleton
from saga.utils.logging.colorstreamhandler import *
from saga.utils.logging.defaultformatter import DefaultFormatter

class _LoggerConfig(object):
    __metaclass__ = Singleton

    def __init__(self):
        saga_verbose = os.environ.get('SAGA_VERBOSE')
        if saga_verbose is not None:
            pass
        else:
            self._loglevel = DEBUG

        saga_log_level = os.environ.get('SAGA_LOG_LEVEL')
        if saga_log_level is not None:
            pass
        else:
            self._loglevel = DEBUG

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

def getSAGALogger(module, obj=None):
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

    _logger.setLevel(_LoggerConfig().loglevel)
    _logger.propagate = 0 # Don't bubble up to the root logger

    return _logger

def _test_():
    l1 = getSAGALogger('core')
    l1.info('hi')
    l2 = getSAGALogger('adaptor', 'pbs')
    l2.error('problem')

