
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides log handler management for SAGA.
'''

import os
from   logging import StreamHandler, Filter, getLogger as logging_getLogger
from   logging import INFO, DEBUG, ERROR, WARNING, CRITICAL

from   saga.utils.logger.colorstreamhandler import *
from   saga.utils.logger.filehandler        import FileHandler
from   saga.utils.logger.defaultformatter   import DefaultFormatter
from   saga.utils.singleton                 import Singleton
from   saga.utils.exception                 import ExceptionBase
from   saga.utils.exception                 import get_traceback as get_tb

from   saga.engine.config                   import Configurable

############# forward call to exception util's get_traceback() #################
##
def get_traceback (limit=2) :
    """ 
    :todo: it would be nice to get the following to work::

      self.logger = getLogger()
      self.logger.debug (self.logger.traceback)

    Alas, as we don't inherit the system logger, we can't (easily) add
    properties or methods...
    """
    return get_tb (limit)


############# These are all supported options for saga.logging #################
##
_all_logging_options = [
    { 
    'category'      : 'saga.engine.logging',
    'name'          : 'level', 
    'type'          : str, 
    'default'       : 'CRITICAL', 
    'valid_options' : ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
    'documentation' : 'The log level',
    'env_variable'  : 'SAGA_VERBOSE'
    },
    { 
    'category'      : 'saga.engine.logging',
    'name'          : 'filters', 
    'type'          : list, 
    'default'       : [], 
    'valid_options' : None,
    'documentation' : 'The log filters',
    'env_variable'  : 'SAGA_LOG_FILTER' 
    },
    { 
    'category'      : 'saga.engine.logging',
    'name'          : 'targets', 
    'type'          : list, 
    'default'       : ['STDOUT'], 
    'valid_options' : None,
    'documentation' : 'The log targets',
    'env_variable'  : 'SAGA_LOG_TARGETS' 
    },
    { 
    'category'      : 'saga.engine.logging',
    'name'          : 'ttycolor', 
    'type'          : bool, 
    'default'       : True, 
    'valid_options' : [True, False],
    'documentation' : 'Whether to use colors for console output or not.',
    'env_variable'  : None 
    },
]

################################################################################
##
class Logger(Configurable):
    """
    :todo: documentation.  Also, documentation of options are insufficient
    (like, what are valid options for 'target'?)
    """

    __metaclass__ = Singleton

    class _MultiNameFilter(Filter):
        def __init__(self, names):
            self._names = names
        def filter(self, record):
            for n in self._names:
                if n in record.name:
                    return True

    def __init__(self):

        Configurable.__init__(self, 'saga.engine.logging', _all_logging_options)    
        cfg = self.get_config()

        self._loglevel = cfg['level'].get_value()
        self._filters  = cfg['filters'].get_value()
        self._targets  = cfg['targets'].get_value()

        self._handlers = list()

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

        # create the  handler
        # check how many handlers we need
        for target in self._targets:
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

            if self._filters != []:
                handler.addFilter(_MultiNameFilter(Logger().filters))
            self._handlers.append(handler)


    @property
    def loglevel(self):
        return self._loglevel

    @property
    def filters(self):
        return self._filters

    @property
    def targets(self):
        return self._targets

    @property
    def handlers(self):
        return self._handlers

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

    Logger ()

    if obj is None:
        _logger = logging_getLogger('%-20s' % module)
    else:
        _logger = logging_getLogger('%s.%s' % (module, obj))

    _logger.setLevel(Logger().loglevel)
    _logger.propagate = 0 # Don't bubble up to the root logger

    if _logger.handlers == []:
        for handler in Logger().handlers:
            _logger.addHandler(handler)

    return _logger

################################################################################
##
class LoggingException(ExceptionBase):
    pass

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

