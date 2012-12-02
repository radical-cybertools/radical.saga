# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides a stream handler for the Python logging framework that uses 
    colors to distinguish severity levels.
'''

from logging import StreamHandler, DEBUG, getLogger, Formatter
 
try:
    from colorama import Fore, Back, init, Style
 
    class ColorStreamHandler(StreamHandler):
        """ A colorized output SteamHandler """
 
        # Some basic colour scheme defaults
        colours = {
            'DEBUG' : Fore.CYAN,
            'INFO' : Fore.GREEN,
            'WARN' : Fore.YELLOW,
            'WARNING': Fore.YELLOW,
            'ERROR': Fore.RED,
            'CRIT' : Back.RED + Fore.WHITE,
            'CRITICAL' : Back.RED + Fore.WHITE
        }
 
        @property
        def is_tty(self):
            """ Check if we are using a "real" TTY. If we are not using a TTY it means that
            the colour output should be disabled.
 
            :return: Using a TTY status
            :rtype: bool
            """
            try: return getattr(self.stream, 'isatty', None)()
            except: return False
 
        def emit(self, record):
            try:
                message = self.format(record)
                if not self.is_tty:
                    self.stream.write(message)
                else:
                    self.stream.write(self.colours[record.levelname] + message + Style.RESET_ALL)
                self.stream.write(getattr(self, 'terminator', '\n'))
                self.flush()
            except (KeyboardInterrupt, SystemExit):
                raise
            except:
                self.handleError(record)
 
    has_color_stream_handler = True
except:
    has_color_stream_handler = False
 
def _test_():
    """ Get and initialize a colourised logging instance if the system supports
    it as defined by the log.has_colour
 
    :param name: Name of the logger
    :type name: str
    :param fmt: Message format to use
    :type fmt: str
    :return: Logger instance
    :rtype: Logger
    """
    log = getLogger('testlogger')
    # Only enable colour if support was loaded properly
    handler = ColorStreamHandler() if has_color_stream_handler else StreamHandler()
    handler.setLevel(DEBUG)
    #handler.setFormatter(Formatter(fmt))
    log.addHandler(handler)
    log.setLevel(DEBUG)
    log.propagate = 0 # Don't bubble up to the root logger
    log.debug('DEBUG')
    log.info('INFO')
    log.warning('WARNING')
    log.error('ERROR')
    log.critical('CRITICAL')
