
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA Job Service CPI """

from   saga.cpi.base   import Base
from   saga.cpi.base   import CPI_SYNC_CALL  as sync
from   saga.cpi.base   import CPI_ASYNC_CALL as async
from   saga.cpi.async  import Async


class Service (Base, Async) :

    @sync
    def init_instance              (self, rm, session)         : pass
    @async
    def init_instance_async        (self, rm, session)         : pass

    @sync
    def create_job                 (self, jd, ttype)           : pass
    @async
    def create_job_async           (self, jd, ttype)           : pass

    @sync
    def run_job                    (self, cmd, host, ttype)    : pass
    @async
    def run_job_async              (self, cmd, host, ttype)    : pass

    @sync
    def list                       (self, ttype)               : pass
    @async
    def list_async                 (self, ttype)               : pass

    @sync
    def get_url                    (self, ttype)               : pass
    @async
    def get_url_async              (self, ttype)               : pass

    @sync
    def get_job                    (self, job_id, ttype)       : pass
    @async
    def get_job_async              (self, job_id, ttype)       : pass

    @sync
    def get_self                   (self, ttype)               : pass
    @async
    def get_self_async             (self, ttype)               : pass

    @sync
    def container_run              (self, jobs)                : pass
    @async
    def container_run_async        (self, jobs)                : pass

    @sync
    def container_wait             (self, jobs, mode, timeout) : pass
    @async
    def container_wait_async       (self, jobs, mode, timeout) : pass

    @sync
    def container_cancel           (self, jobs)                : pass
    @async
    def container_cancel_async     (self, jobs)                : pass

    @sync
    def container_get_states       (self, jobs)                : pass
    @async
    def container_get_states_async (self, jobs)                : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

