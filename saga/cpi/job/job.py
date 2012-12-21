# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA Job CPI 
"""

from saga.cpi.base import Base

import inspect
import saga.exceptions

# class Job (CPIBase) : # CPIObject, CPIAsync, CPIAttributes, CPIPermissions) :
class Job (Base) :
    
    def __init__ (self, api) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def init_instance (self, info) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def init_instance_async (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def get_id (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def get_description (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def get_stdin (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def get_stdout (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def get_stderr (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def suspend (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def resume (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def checkpoint (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def migrate (self, jd, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def signal (self, signum, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))


    #-----------------------------------------------------------------
    # task methods flattened into job :-/
    def run (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def cancel (self, timeout, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def wait (self, timeout, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def get_state (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def get_result (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def get_object (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def re_raise (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))


    #-----------------------------------------------------------------
    # attribute getters
    def get_exit_code (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def get_started (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def get_finished (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def get_execution_hosts (self, ttype) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))


# class Self (Job, monitoring.Steerable) :
class Self (Job) :

    def __init__(self):
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

