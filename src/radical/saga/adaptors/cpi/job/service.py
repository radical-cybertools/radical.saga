
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" Provides the SAGA Job Service CPI """

from ..decorators import CPI_SYNC_CALL  as SYNC
from ..decorators import CPI_ASYNC_CALL as ASYNC
from ..base       import CPIBase
from ..sasync     import Async


class Service (CPIBase, Async) :

    # ----------------------------------------------------------------
    #
    # initialization methods
    #
    def __init__ (self, api, adaptor) :

        _cpi_base = super  (Service, self)
        _cpi_base.__init__ (api, adaptor)

    @SYNC
    def init_instance              (self, rm, session)         : pass
    @ASYNC
    def init_instance_async        (self, rm, session)         : pass

    @SYNC
    def close                      (self)                      : pass


    # ----------------------------------------------------------------
    #
    # job service methods
    #
    @SYNC
    def create_job                 (self, jd, ttype)           : pass
    @ASYNC
    def create_job_async           (self, jd, ttype)           : pass

    @SYNC
    def run_job                    (self, cmd, host, ttype)    : pass
    @ASYNC
    def run_job_async              (self, cmd, host, ttype)    : pass

    @SYNC
    def list                       (self, ttype)               : pass
    @ASYNC
    def list_async                 (self, ttype)               : pass

    @SYNC
    def get_url                    (self, ttype)               : pass
    @ASYNC
    def get_url_async              (self, ttype)               : pass

    @SYNC
    def get_job                    (self, job_id, ttype)       : pass
    @ASYNC
    def get_job_async              (self, job_id, ttype)       : pass

    @SYNC
    def get_self                   (self, ttype)               : pass
    @ASYNC
    def get_self_async             (self, ttype)               : pass

    @SYNC
    def container_run              (self, jobs)                : pass
    @ASYNC
    def container_run_async        (self, jobs)                : pass

    @SYNC
    def container_wait             (self, jobs, mode, timeout) : pass
    @ASYNC
    def container_wait_async       (self, jobs, mode, timeout) : pass

    @SYNC
    def container_cancel           (self, jobs, timeout)       : pass
    @ASYNC
    def container_cancel_async     (self, jobs, timeout)       : pass

    @SYNC
    def container_get_states       (self, jobs)                : pass
    @ASYNC
    def container_get_states_async (self, jobs)                : pass




