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
from saga.utils.exception import BaseException
from saga.utils.logging.colorstreamhandler import *
from saga.utils.logging.filehandler import FileHandler
from saga.utils.logging.defaultformatter import DefaultFormatter

class Config(object):
    __metaclass__ = Singleton

    def __init__(self):
        
        # the default log level
        self._loglevel = CRITICAL # default loglevel
        self._filters = None
        self._targets = ['STDOUT'] # default log target

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

        SAGA_LOG_FILTERS = os.environ.get('SAGA_LOG_FILTERS')
        if SAGA_LOG_FILTERS is not None:
            try:
                self._filters = list() # reset filters
                for filt in SAGA_LOG_FILTERS.split(','):
                    self._filters.append(filt)
            except Exception, ex:
                raise LoggingException('%s is not a valid value for SAGA_LOG_FILTERS.' % SAGA_LOG_FILTERS)

        SAGA_LOG_TARGETS = os.environ.get('SAGA_LOG_TARGETS')
        if SAGA_LOG_TARGETS is not None:
            try:
                self._targets = list() # reset filters
                for target in SAGA_LOG_TARGETS.split(','):
                    self._targets.append(target)
            except Exception, ex:
                raise LoggingException('%s is not a valid value for SAGA_LOG_TARGETS.' % SAGA_LOG_TARGETS)

    @property
    def loglevel(self):
        return self._loglevel

    @property
    def filters(self):
        return self._filters

    @property
    def targets(self):
        return self._targets

class LoggingException(BaseException):
    pass

class _MultiNameFilter(Filter):

    def __init__(self, names):
        self._names = names

    def filter(self, record):
        for n in self._names:
            if n in record.name:
                return True

def getLogger(module, obj=None):
    ''' Get the SAGA logger.
    '''
    if obj is None:
        _logger = logging_getLogger('saga.%s' % module)
    else:
        _logger = logging_getLogger('saga.%s.%s' % (module, obj))

    # check how many handlers we need
    for target in Config().targets:
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
        if Config().filters is not None:
            handler.addFilter(_MultiNameFilter(Config().filters))

        _logger.addHandler(handler)

    _logger.setLevel(Config().loglevel)
    _logger.propagate = 0 # Don't bubble up to the root logger

    return _logger

def testMe():

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



