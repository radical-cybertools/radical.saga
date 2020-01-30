
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2020, The SAGA Project"
__license__   = "MIT"


''' no operation (noop) based job adaptor implementation '''

import time
import threading

from ...               import exceptions as rse
from ..                import base
from ..cpi             import SYNC_CALL
from ..cpi             import job as cpi
from ...               import job as api


# ------------------------------------------------------------------------------
#
class _job_state_monitor(threading.Thread):

    # --------------------------------------------------------------------------
    #
    def __init__(self, log):

        self._log  = log
        self._lock = threading.Lock()
        self._term = threading.Event()
        self._jobs = dict()
        self._cnt  = 0

        super(_job_state_monitor, self).__init__()

        self.setDaemon(True)


    # --------------------------------------------------------------------------
    #
    def stop(self):

        self._term.set()


    # --------------------------------------------------------------------------
    #
    def add_job(self, job):

        job._id = 'job.%06d' % self._cnt
        self._cnt += 1

        assert(job._id not in self._jobs)
        job._set_state(api.RUNNING)
        job._started = time.time()

        with self._lock:
            self._jobs[job._id] = job


    # --------------------------------------------------------------------------
    #
    def get_job(self, jid):

        assert(jid in self._jobs)
        with self._lock:
            return self._jobs[jid]


    # --------------------------------------------------------------------------
    #
    def list_jobs(self):

        with self._lock:
            return list(self._jobs.keys())


    # --------------------------------------------------------------------------
    #
    def run(self):

        try:

            while not self._term.is_set():

                now  = time.time()
                keep = dict()

                with self._lock:

                    for job in self._jobs.values():

                        if job.get_state() == api.CANCELED:
                            continue

                        if job.tgt < now:
                            job._finished  = now
                            job._exit_code = 0
                            job._set_state(api.DONE)

                        else:
                            keep[job._id] = job

                    self._jobs = keep

                time.sleep(0.1)

        except Exception:

            self._log.exception("Exception in job monitoring thread")


# ------------------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "radical.saga.adaptors.noop_job"
_ADAPTOR_SCHEMAS       = ["noop"]



# ------------------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES  = {
    "jdes_attributes" : [api.NAME,
                         api.EXECUTABLE,
                         api.PRE_EXEC,
                         api.POST_EXEC,
                         api.ARGUMENTS,
                         api.ENVIRONMENT,
                         api.WORKING_DIRECTORY,
                         api.FILE_TRANSFER,
                         api.INPUT,
                         api.OUTPUT,
                         api.ERROR,
                         api.NAME,
                         api.WALL_TIME_LIMIT,
                         api.TOTAL_CPU_COUNT,
                         api.TOTAL_GPU_COUNT,
                         api.PROCESSES_PER_HOST,
                         api.SPMD_VARIATION,
                        ],
    "job_attributes"  : [api.EXIT_CODE,
                         api.EXECUTION_HOSTS,
                         api.CREATED,
                         api.STARTED,
                         api.FINISHED],
    "metrics"         : [api.STATE,
                         api.STATE_DETAIL],
    "contexts"        : {}
}

# ------------------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC           = {
    "name"            : _ADAPTOR_NAME,
    "capabilities"    : _ADAPTOR_CAPABILITIES,
    "description"     : '''
        The Noop job adaptor, which fakes job execution and is used for testing
        and benchmarking purposes.''',
    "example"         : "examples/jobs/noopjob.py",
    "schemas"         : {"noop"  : "fake job execution"}
}

# ------------------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO = {
    "name"            : _ADAPTOR_NAME,
    "version"         : "v0.1",
    "schemas"         : _ADAPTOR_SCHEMAS,
    "capabilities"    : _ADAPTOR_CAPABILITIES,
    "cpis"            : [{
                                 "type" : "radical.saga.job.Service",
                                 "class": "NoopJobService"
                         },{
                                 "type" : "radical.saga.job.Job",
                                 "class": "NoopJob"
                         }]
}


# ------------------------------------------------------------------------------
# The adaptor class
class Adaptor(base.Base):
    '''
    This is the actual adaptor class, which gets loaded by SAGA (i.e., by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self):

        base.Base.__init__(self, _ADAPTOR_INFO, expand_env=False)


    # --------------------------------------------------------------------------
    #
    def sanity_check(self):

        pass


# ------------------------------------------------------------------------------
#
class NoopJobService(cpi.Service):

    # --------------------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        _cpi_base = super (NoopJobService, self)
        _cpi_base.__init__(api, adaptor)


    # --------------------------------------------------------------------------
    #
    def __del__(self):

        try   : self.close()
        except: pass


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, rm_url, session):
        '''
        Service instance constructor
        '''

        self.rm      = rm_url
        self.session = session
        self.jobs    = dict()

        # Use `_set_session` method of the base class to set the session object.
        # `_set_session` and `get_session` methods are provided by `CPIBase`.
        self._set_session(session)

        # the monitoring thread - one per service instance.
        self.monitor = _job_state_monitor(self._logger)
        self.monitor.start()

        return self.get_api()


    # --------------------------------------------------------------------------
    #
    def close(self):

        if self.monitor:
            self.monitor.stop()


    # --------------------------------------------------------------------------
    #
    #
    def _job_run(self, job):
        '''
        runs a job on the wrapper via pty, and returns the job id
        '''

        if not job.jd.executable.endswith('sleep'):
            raise ValueError('expected "sleep", not %s' % job.jd.executable)


        if len(job.jd.arguments) != 1:
            raise ValueError('expected int argument, not %s' % job.jd.arguments)

        job.tgt = time.time() + int(job.jd.arguments[0])
        self.monitor.add_job(job)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def run_job(self, cmd, host):
        '''
        Implements adaptors.cpi.job.Service.run_job()
        '''

        if not cmd:
            raise rse.BadParameter._log(self._logger,
                    "run_job needs a command to run")

        if host and host != self.rm.host:
            raise rse.BadParameter._log(self._logger,
                    "Can only run jobs on %s, not on %s" % (self.rm.host, host))

        jd = api.Description()

        exe, arg = cmd.split()

        jd.executable = exe
        jd.arguments  = [arg]

        job = self.create_job(jd)
        job.run()

        return job


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def create_job(self, jd):

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service"    : self,
                         "job_description": jd,
                         "job_schema"     : self.rm.schema }

        return api.Job(_adaptor=self._adaptor, _adaptor_state=adaptor_state)


    # --------------------------------------------------------------------------
    @SYNC_CALL
    def get_url(self):

        return self.rm


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self):

        return self.monitor.list_jobs()


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job(self, job_id, no_reconnect=False):

        return self.monitor.get_job(job_id)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def container_run(self, jobs):
        '''
        From all the job descriptions in the container, build a bulk, and submit
        as async.
        '''

        self._logger.debug("container run: %s"  %  str(jobs))

        for job in jobs:
            job.run()


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def container_wait(self, jobs, mode, timeout):

        for job in jobs:

            job.wait()


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def container_cancel(self, jobs, timeout):

        for job in jobs:

            job.cancel()


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def container_get_states(self, jobs):

        return [job.get_state() for job in jobs]


# ------------------------------------------------------------------------------
#
class NoopJob(cpi.Job):

    # --------------------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        _cpi_base = super (NoopJob, self)
        _cpi_base.__init__(api, adaptor)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, job_info):

        if 'job_description' in job_info:
            # comes from job.service.create_job()
            self.js = job_info["job_service"]
            self.jd = job_info["job_description"]

            # initialize job attribute values
            self._id              = None
            self._name            = self.jd.get(api.NAME)
            self._log             = list()
            self._state           = None
            self._exit_code       = None
            self._exception       = None
            self._created         = time.time()
            self._name            = self.jd.name
            self._started         = None
            self._finished        = None

            self._set_state(api.NEW)

        elif 'job_id' in job_info:
            # initialize job attribute values
            self.js               = job_info["job_service"]
            self.jd               = None
            self._id              = job_info['job_id']
            self._name            = job_info.get('job_name')
            self._log             = list()
            self._state           = None
            self._exit_code       = None
            self._exception       = None
            self._created         = None
            self._name            = None
            self._started         = None
            self._finished        = None

        else:
            # don't know what to do...
            raise rse.BadParameter("insufficient info for job creation")

        if self._created: self._created = float(self._created)

        return self.get_api()


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_description(self):

        return self.jd


    # --------------------------------------------------------------------------
    #
    def _update_state(self, state):

        # update state, and report to application
        self._state = state
        self._api()._attributes_i_set('state', self._state, self._api()._UP)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        '''
        Implements adaptors.cpi.job.Job.get_state()
        '''

        # may not yet have backend representation, state is 'NEW'
        if self._id is None:
            return self._state

        return self._state


    # --------------------------------------------------------------------------
    #
    def _set_state(self, state):

        old_state = self._state

        # on state changes, trigger notifications
        if old_state != state:
            self._state  = state
            self._api()._attributes_i_set('state', state, self._api()._UP)

        return self._state


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created(self):

        return self._created


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started(self):

        return self._started


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished(self):

        return self._finished


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_stdout(self):

        return ''


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_stderr(self):

        return ''


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_log(self):

        return ''


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_service_url(self):

        if not self.js:
            raise rse.IncorrectState("Job Service URL unknown")
        else:
            return self.js.get_url()


    # --------------------------------------------------------------------------
    #
    # TODO: this should also fetch the(final) state, to safe a hop
    # TODO: implement via notifications
    #
    @SYNC_CALL
    def wait(self, timeout):
        '''
        A call to the noop to do the WAIT would block the noop for any
        other interactions.  In particular, it would practically kill it if the
        Wait waits forever...

        So we implement the wait via a state pull.  The *real* solution is to
        implement state notifications, and wait for such a notification to
        arrive within timeout seconds...
        '''

        time_start = time.time()
        time_now   = time_start

        while True:

            state = self.get_state()

            if state in [api.DONE, api.FAILED, api.CANCELED]:
                return True

            # avoid busy poll
            # FIXME: self-tune by checking call latency
            time.sleep(0.1)

            # check if we hit timeout
            if timeout >= 0:
                time_now = time.time()
                if time_now - time_start > timeout:
                    return False


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id(self):

        return self._id


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_name(self):

        return self._name


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code(self):

        return self._exit_code


    # --------------------------------------------------------------------------
    #
    # TODO: the values below should be fetched with every get_state...
    #
    @SYNC_CALL
    def get_execution_hosts(self):

        return [self.js.get_url().host]


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):

        self.js._job_run(self)
        self.js.jobs[self._id] = self._api()

        self._set_state(api.RUNNING)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def suspend(self):

        if self.get_state() != api.RUNNING:
            raise rse.IncorrectState("Cannot suspend, job is not RUNNING")

        self._old_state = self.get_state()
        self._adaptor._update_state(api.SUSPENDED)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def resume(self):

        if self.get_state() != api.SUSPENDED:
            raise rse.IncorrectState("Cannot resume, job is not SUSPENDED")

        self._adaptor._update_state(self._old_state)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):

        if self.get_state() not in [api.RUNNING,  api.SUSPENDED,
                                    api.CANCELED, api.DONE,
                                    api.FAILED]:
            raise rse.IncorrectState("Cannot cancel, job is not running")

        if self._state in [api.CANCELED, api.DONE, api.FAILED]:
            self._set_state(api.CANCELED)
            return


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def re_raise(self):
        # nothing to do here actually, as run() is synchronous...
        return self._exception


# ------------------------------------------------------------------------------

