# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

''' Provides exception handling utilities for SAGA.
'''

try: 
    from colorama import Fore, Back, init, Style
    RED = Fore.RED
    RES = Style.RESET_ALL
except:
    RED = ""
    RES = ""

class BaseException(Exception):
    def __init__(self, message):

        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, message)

    def __str__(self):
        return RED+self.message+RES
