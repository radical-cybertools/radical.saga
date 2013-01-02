# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA Job CPI 
"""

from   saga.cpi.base   import Base, CPI_SYNC_CALL, CPI_ASYNC_CALL
from   saga.cpi.async  import Async

class Job (Base, Async) :
    
    @CPI_SYNC_CALL
    def __init__ (self, api) :
        pass

    @CPI_SYNC_CALL
    def init_instance (self, info) :
        pass

    @CPI_SYNC_CALL
    def init_instance_async (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_id (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_description (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_stdin (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_stdout (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_stderr (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def suspend (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def resume (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def checkpoint (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def migrate (self, jd, ttype) :
        pass

    @CPI_SYNC_CALL
    def signal (self, signum, ttype) :
        pass


    #-----------------------------------------------------------------
    # task methods flattened into job :-/
    @CPI_SYNC_CALL
    def run (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def cancel (self, timeout, ttype) :
        pass

    @CPI_SYNC_CALL
    def wait (self, timeout, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_state (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_result (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_object (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def re_raise (self, ttype) :
        pass


    #-----------------------------------------------------------------
    # attribute getters
    @CPI_SYNC_CALL
    def get_exit_code (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_created (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_started (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_finished (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_execution_hosts (self, ttype) :
        pass


# class Self (Job, monitoring.Steerable) :
class Self (Job) :

    @CPI_SYNC_CALL
    def __init__(self):
        pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

