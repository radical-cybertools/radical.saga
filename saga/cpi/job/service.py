
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA Job Service CPI 
"""

from   saga.cpi.base   import Base, CPI_SYNC_CALL, CPI_ASYNC_CALL
from   saga.cpi.async  import Async

# class Service (Object, Async, Configurable) :
class Service (Base, Async) :

    @CPI_SYNC_CALL
    def __init__ (self, api) : 
        pass

    @CPI_SYNC_CALL
    def init_instance (self, rm, session) :
        pass

    @CPI_SYNC_CALL
    def init_instance_async (self, rm, session, ttype) :
        pass

    @CPI_SYNC_CALL
    def create_job (self, jd, ttype) :
        pass

    @CPI_SYNC_CALL
    def run_job (self, cmd, host, ttype) :
        pass

    @CPI_SYNC_CALL
    def list (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_url (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_job (self, job_id, ttype) :
        pass

    @CPI_SYNC_CALL
    def get_self (self, ttype) :
        pass

    @CPI_SYNC_CALL
    def container_run (self, jobs) :
        pass

    @CPI_SYNC_CALL
    def container_wait (self, jobs, mode, timeout) :
        pass

    @CPI_SYNC_CALL
    def container_cancel (self, jobs) :
        pass

    @CPI_SYNC_CALL
    def container_get_states (self, jobs) :
        pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

