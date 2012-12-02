# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, Ole Christian Weidner"
__license__   = "MIT"

''' Provides a CommandLineWrapper implementation based on simple SSH 
    tunneling via a modified pexssh library.
'''

class SSHCommandLineWrapper(object):

    def __init__(self):
        raise Exception('Not Implemented.')

    def open(self):
        raise Exception('Not Implemented.')

    def close(self):
        raise Exception('Not Implemented.')

    def run_sync(self, executable, arguments, environemnt):
        raise Exception('Not Implemented.')
