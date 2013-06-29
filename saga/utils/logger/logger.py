
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


''' Provides log handler management for SAGA.
'''

import logging
import saga.exceptions as se

from   saga.utils.config                    import Configurable
from   saga.utils.logger.colorstreamhandler import *
from   saga.utils.logger.filehandler        import FileHandler
from   saga.utils.logger.defaultformatter   import DefaultFormatter
from   saga.utils.singleton                 import Singleton


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
class _Logger(Configurable):
    """
    :todo: documentation.  Also, documentation of options are insufficient
    (like, what are valid options for 'target'?)

    This class is not to be directly used by applications.
    """

    __metaclass__ = Singleton

    class _MultiNameFilter(logging.Filter):
        def __init__(self, pos_filters, neg_filters=[]):
            self._pos_filters = pos_filters
            self._neg_filters = neg_filters

        def filter(self, record):
            print_it = False

            if not len(self._pos_filters) :
                print_it = True
            else :
                for f in self._pos_filters:
                    if  f in record.name:
                        print_it = True

            for f in self._neg_filters:
                if  f in record.name:
                    print_it = False

            return print_it

    def __init__(self):

        Configurable.__init__(self, 'saga.engine.logging', _all_logging_options)    
        cfg = self.get_config()

        self._loglevel = str(cfg['level'].get_value())
        self._filters  = cfg['filters'].get_value()
        self._targets  = cfg['targets'].get_value()
        self._handlers = list()

        if self._loglevel is not None:
            if self._loglevel.isdigit():
                if   int(self._loglevel)    >= 4:           self._loglevel = logging.DEBUG
                elif int(self._loglevel)    == 3:           self._loglevel = logging.INFO
                elif int(self._loglevel)    == 2:           self._loglevel = logging.WARNING
                elif int(self._loglevel)    == 1:           self._loglevel = logging.ERROR
                elif int(self._loglevel)    == 0:           self._loglevel = logging.CRITICAL
                else: raise se.NoSuccess('%s is not a valid value for SAGA_VERBOSE.' % self._loglevel)
            else:
                if   self._loglevel.lower() == 'debug':     self._loglevel = logging.DEBUG
                elif self._loglevel.lower() == 'info':      self._loglevel = logging.INFO
                elif self._loglevel.lower() == 'warning':   self._loglevel = logging.WARNING
                elif self._loglevel.lower() == 'error':     self._loglevel = logging.ERROR
                elif self._loglevel.lower() == 'critical':  self._loglevel = logging.CRITICAL
                else: raise se.NoSuccess('%s is not a valid value for SAGA_VERBOSE.' % self._loglevel)

        # create the handlers (target + formatter + filter)
        for target in self._targets:

            if target.lower() == 'stdout':
                # create a console stream logger
                # Only enable colour if support was loaded properly
                if has_color_stream_handler is True:
                    handler = ColorStreamHandler()
                else: 
                    handler = logging.StreamHandler()
            else:
                # got to be a file logger
                handler = FileHandler(target)

            handler.setFormatter(DefaultFormatter)

            if self._filters != []:
                pos_filters = []
                neg_filters = []

                for f in self._filters :
                    if  f and f[0] == '!' :
                        neg_filters.append (f[1:])
                    else :
                        pos_filters.append (f)

                handler.addFilter(self._MultiNameFilter (pos_filters, neg_filters))

            self._handlers.append(handler)


    @property
    def loglevel(self):
        return self._loglevel

    @property
    def handlers(self):
        return self._handlers


################################################################################
#
# FIXME: strange pylint error
#
def getLogger (name='saga-python'):
    ''' Get a SAGA logger.  For any new name, a new logger instance will be
    created; subsequent calls to this method with the same name argument will
    return the same instance.'''

    # make sure the saga logging configuration is evaluated at least once
    _Logger ()

    # get a python logger
    _logger = logging.getLogger(name)

    # was this logger initialized before?
    #
    # The check / action below forms a race condition, but (a) this should be
    # rare (TM), and (b) it does have no practical consequences (apart from a 
    # small runtime overhead).  So, we don't care... :-P
    if _logger.handlers == []:

        # initialize that logger
        for handler in _Logger().handlers:
            _logger.addHandler(handler)

        _logger.setLevel(_Logger().loglevel)
        _logger.propagate = 0 # Don't bubble up to the root logger

    # setup done - we can return the logger
    return _logger


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

