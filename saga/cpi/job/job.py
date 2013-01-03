
__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA Job CPI """

from   saga.cpi.base   import CPIBase
from   saga.cpi.base   import CPI_SYNC_CALL  as sync
from   saga.cpi.base   import CPI_ASYNC_CALL as async
from   saga.cpi.async  import Async


class Job (CPIBase, Async) :

    @sync
    def init_instance             (self, info)           : pass
    @async
    def init_instance_async       (self, info)           : pass

    @sync
    def get_id                    (self, ttype)          : pass
    @async
    def get_id_async              (self, ttype)          : pass

    @sync
    def get_description           (self, ttype)          : pass
    @async
    def get_description_async     (self, ttype)          : pass

    @sync
    def get_stdin                 (self, ttype)          : pass
    @async
    def get_stdin_async           (self, ttype)          : pass

    @sync
    def get_stdout                (self, ttype)          : pass
    @async
    def get_stdout_async          (self, ttype)          : pass

    @sync
    def get_stderr                (self, ttype)          : pass
    @async
    def get_stderr_async          (self, ttype)          : pass

    @sync
    def suspend                   (self, ttype)          : pass
    @async
    def suspend_async             (self, ttype)          : pass

    @sync
    def resume                    (self, ttype)          : pass
    @async
    def resume_async              (self, ttype)          : pass

    @sync
    def checkpoint                (self, ttype)          : pass
    @async
    def checkpoint_async          (self, ttype)          : pass

    @sync
    def migrate                   (self, jd, ttype)      : pass
    @async
    def migrate_async             (self, jd, ttype)      : pass

    @sync
    def signal                    (self, signum, ttype)  : pass
    @async
    def signal_async              (self, signum, ttype)  : pass


    #-----------------------------------------------------------------
    # task methods flattened into job :-/
    @sync
    def run                       (self, ttype)          : pass
    @async
    def run_async                 (self, ttype)          : pass

    @sync
    def cancel                    (self, timeout, ttype) : pass
    @async
    def cancel_async              (self, timeout, ttype) : pass

    @sync
    def wait                      (self, timeout, ttype) : pass
    @async
    def wait_async                (self, timeout, ttype) : pass

    @sync
    def get_state                 (self, ttype)          : pass
    @async
    def get_state_async           (self, ttype)          : pass

    @sync
    def get_result                (self, ttype)          : pass
    @async
    def get_result_async          (self, ttype)          : pass

    @sync
    def get_object                (self, ttype)          : pass
    @async
    def get_object_async          (self, ttype)          : pass

    @sync
    def re_raise                  (self, ttype)          : pass
    @async
    def re_raise_async            (self, ttype)          : pass


    #-----------------------------------------------------------------
    # attribute getters
    @sync
    def get_exit_code             (self, ttype)          : pass
    @async
    def get_exit_code_async       (self, ttype)          : pass

    @sync
    def get_created               (self, ttype)          : pass
    @async
    def get_created_async         (self, ttype)          : pass

    @sync
    def get_started               (self, ttype)          : pass
    @async
    def get_started_async         (self, ttype)          : pass

    @sync
    def get_finished              (self, ttype)          : pass
    @async
    def get_finished_async        (self, ttype)          : pass

    @sync
    def get_execution_hosts       (self, ttype)          : pass
    @async
    def get_execution_hosts_async (self, ttype)          : pass


# class Self (Job, monitoring.Steerable) :
class Self (Job) :

    @sync
    def init_instance             (self)                 : pass
    @async
    def init_instance_async       (self)                 : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

