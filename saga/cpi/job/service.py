# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA Job Service CPI 
"""

from saga.cpi.base import Base

import saga.exceptions

# class Service (Object, Async, Configurable) :
class Service (Base) :

    def __init__ (self, api) : 
        raise saga.exceptions.NotImplemented ("method not implemented")

    def init_instance (self, rm, session) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def init_instance_async (self, rm, session, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def create_job (self, jd, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def run_job (self, cmd, host, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def list (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_url (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_job (self, job_id, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_self (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def container_run (self, jobs) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def container_wait (self, jobs, mode, timeout) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def container_cancel (self, jobs) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def container_get_states (self, jobs) :
        raise saga.exceptions.NotImplemented ("method not implemented")

