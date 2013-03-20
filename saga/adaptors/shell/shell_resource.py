
""" shell based resource adaptor implementation """

import saga.utils.which
import saga.utils.pty_shell

import saga.adaptors.cpi.base
import saga.adaptors.cpi.resource

from   saga.resource.constants import *
ANY = COMPUTE | STORAGE

import re
import os
import time
import threading

SYNC_CALL  = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL


# --------------------------------------------------------------------
# the adaptor info
#
_ADAPTOR_NAME          = "saga.adaptor.shell_resource"
_ADAPTOR_SCHEMAS       = ["local", "shell"]
_ADAPTOR_OPTIONS       = []

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES  = {
    "rdes_attributes"  : [saga.resource.RTYPE         ,
                          saga.resource.MACHINE_OS    ,
                          saga.resource.MACHINE_ARCH  ,
                          saga.resource.SIZE          ,
                          saga.resource.MEMORY        ,
                          saga.resource.ACCESS       ],
    "res_attributes"   : [saga.resource.RTYPE         ,
                          saga.resource.MACHINE_OS    ,
                          saga.resource.MACHINE_ARCH  ,
                          saga.resource.SIZE          ,
                          saga.resource.MEMORY        ,
                          saga.resource.ACCESS       ],    
    "metrics"          : [saga.resource.STATE, 
                          saga.resource.STATE_DETAIL],
    "contexts"         : {"ssh"      : "public/private keypair",
                          "x509"     : "X509 proxy for gsissh",
                          "userpass" : "username/password pair for ssh"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC           = {
    "name"             : _ADAPTOR_NAME,
    "cfg_options"      : _ADAPTOR_OPTIONS, 
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "description"      : """ 
        The Shell resource adaptor. This adaptor attempts to determine what job
        submission endpoint and file system resources are available for a given
        host, and provides the respective access URLs.
        """,
    "example": "examples/jobs/localresource.py",
    "schemas"          : {"shell"  : "find access URLs for shell-job/file adaptors"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    "name"             : _ADAPTOR_NAME,
    "version"          : "v0.1",
    "schemas"          : _ADAPTOR_SCHEMAS,
    "cpis"             : [
        { 
        "type"         : "saga.resource.Manager",
        "class"        : "ShellResourceManager"
        }, 
        { 
        "type"         : "saga.resource.Compute",
        "class"        : "ShellResourceCompute"
        },
        { 
        "type"         : "saga.resource.Storage",
        "class"        : "ShellResourceStorage"
        }
    ]
}

###############################################################################
# The adaptor class

class Adaptor (saga.adaptors.cpi.base.AdaptorBase):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """


    # ----------------------------------------------------------------
    #
    def __init__ (self) :

        saga.adaptors.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)


    # ----------------------------------------------------------------
    #
    def sanity_check (self) :

        # FIXME: also check for gsissh

        pass



###############################################################################
#
class ShellResourceManager (saga.adaptors.cpi.resource.Manager) :

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (ShellResourceManager, self)
        self._cpi_base.__init__ (api, adaptor)



    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, session) :

        self.url     = saga.Url (url)  # deep copy
        self.session = session
        self.access  = {}
        self.access[COMPUTE] = []
        self.access[STORAGE] = []
        self.access[ANY]     = []

        # check for compute entry points
        for schema in ['fork', 'ssh', 'gsissh'] :
            tmp_url = saga.Url (self.url)  # deep copy
            tmp_url.schema = schema

            shell = saga.utils.pty_shell.PTYShell (tmp_url, self.session, self._logger)

            if  shell.alive () :
                self.access[COMPUTE].append (tmp_url)
                self.access[ANY]    .append (tmp_url)
                shell.finalize (True)


        # check for storage entry points
        for schema in ['file', 'sftp', 'gsisftp'] :
            tmp_url = saga.Url (self.url)  # deep copy
            tmp_url.schema = schema

            shell = saga.utils.pty_shell.PTYShell (tmp_url, self.session, self._logger)

            if  shell.alive () :
                self.access[STORAGE].append (tmp_url)
                self.access[ANY]    .append (tmp_url)
                shell.finalize (True)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def acquire (self, rd) :

        if  not rd :
            raise saga.BadParameter._log (self._logger, "acquire needs a resource description")

        if  rd.rtype != COMPUTE and \
            rd.rtype != STORAGE     :
            raise saga.BadParameter._log (self._logger, "can only acquire compute and storage resources.")


        # check that only supported attributes are provided
        for attribute in rd.list_attributes():
            if attribute not in _ADAPTOR_CAPABILITIES["rdes_attributes"]:
                msg = "'resource.Description.%s' is not supported by this adaptor" % attribute
                raise saga.BadParameter._log (self._logger, msg)

        if  rd.access :
            access_url = saga.Url (rd.access) 
            if  not access_url in self.access[rd.rtype] :
                msg = "access 's' is not supported by this backend" % attribute
                raise saga.BadParameter._log (self._logger, msg)

        
        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = { "resource_manager"     : self, 
                          "resource_description" : rd,
                          "resource_schema"      : self.url.schema }

        if rd.rtype == COMPUTE :
            return saga.resource.Compute (_adaptor=self._adaptor, _adaptor_state=adaptor_state)

        if rd.rtype == STORAGE :
            return saga.resource.Storage (_adaptor=self._adaptor, _adaptor_state=adaptor_state)


    # ----------------------------------------------------------------
    @SYNC_CALL
    def get_url (self) :

        return self.url


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list (self, rtype):

        return self.access[rtype]
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job (self, jobid):

        known_jobs = self.list ()

        if jobid not in known_jobs :
            raise saga.BadParameter._log (self._logger, "job id '%s' unknown"
                                       % jobid)

        else:
            # this dict is passed on to the job adaptor class -- use it to pass any
            # state information you need there.
            adaptor_state = { "job_service"     : self, 
                              "job_id"          : jobid,
                              "job_schema"      : self.rm.schema }

            return saga.job.Job (_adaptor=self._adaptor, _adaptor_state=adaptor_state)

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_run (self, jobs) :
        """
        From all the job descriptions in the container, build a bulk, and submit
        as async.  The read whaterver the wrapper returns, and sort through the
        messages, assigning job IDs etc.
        """

        self._logger.debug ("container run: %s"  %  str(jobs))

        bulk = "BULK\n"

        for job in jobs :
            cmd   = self._jd2cmd (job.description)
            bulk += "RUN %s\n" % cmd

        bulk += "BULK_RUN\n"
        self.shell.run_async (bulk)

        for job in jobs :

            ret, out = self.shell.find_prompt ()

            if  ret != 0 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to run job: (%s)(%s)" % (ret, out))
                continue

            lines = filter (None, out.split ("\n"))

            if  len (lines) < 2 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to run job : (%s)(%s)" % (ret, out))
                continue

            if lines[-2] != "OK" :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to run job : (%s)(%s)" % (ret, out))
                continue

            # FIXME: verify format of returned pid (\d+)!
            pid    = lines[-1].strip ()
            job_id = "[%s]-[%s]" % (self.rm, pid)

            self._logger.debug ("started job %s" % job_id)

            self.njobs += 1

            # FIXME: at this point we need to make sure that we actually created
            # the job.  Well, we should make sure of this *before* we run it.
            # But, actually, the container sorter should have done that already?
            # Check!
            job._adaptor._id = job_id

        # we also need to find the output of the bulk op itself
        ret, out = self.shell.find_prompt ()

        if  ret != 0 :
            self._logger.error ("failed to run (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

        lines = filter (None, out.split ("\n"))

        if  len (lines) < 2 :
            self._logger.error ("Cannot evaluate status of bulk job submission: (%s)(%s)" % (ret, out))
            return

        if lines[-2] != "OK" :
            self._logger.error ("failed to run (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_wait (self, jobs, mode, timeout) :

        self._logger.debug ("container wait: %s"  %  str(jobs))

        bulk = "BULK\n"

        for job in jobs :
            rm, pid = self._adaptor.parse_id (job.id)
            bulk   += "WAIT %s\n" % pid

        bulk += "BULK_RUN\n"
        self.shell.run_async (bulk)

        for job in jobs :

            ret, out = self.shell.find_prompt ()

            if  ret != 0 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to wait for job: (%s)(%s)" % (ret, out))
                continue

            lines = filter (None, out.split ("\n"))

            if  len (lines) < 2 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to wait for job : (%s)(%s)" % (ret, out))
                continue

            if lines[-2] != "OK" :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to wait for job : (%s)(%s)" % (ret, out))
                continue

        # we also need to find the output of the bulk op itself
        ret, out = self.shell.find_prompt ()

        if  ret != 0 :
            self._logger.error ("failed to wait for (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

        lines = filter (None, out.split ("\n"))

        if  len (lines) < 2 :
            self._logger.error ("Cannot evaluate status of bulk job wait: (%s)(%s)" % (ret, out))
            return

        if lines[-2] != "OK" :
            self._logger.error ("failed to wait for (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_cancel (self, jobs, timeout) :

        self._logger.debug ("container cancel: %s"  %  str(jobs))

        bulk = "BULK\n"

        for job in jobs :
            rm, pid = self._adaptor.parse_id (job.id)
            bulk   += "CANCEL %s\n" % pid

        bulk += "BULK_RUN\n"
        self.shell.run_async (bulk)

        for job in jobs :

            ret, out = self.shell.find_prompt ()

            if  ret != 0 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to cancel job: (%s)(%s)" % (ret, out))
                continue

            lines = filter (None, out.split ("\n"))

            if  len (lines) < 2 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to cancel job : (%s)(%s)" % (ret, out))
                continue

            if lines[-2] != "OK" :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to cancel job : (%s)(%s)" % (ret, out))
                continue

        # we also need to find the output of the bulk op itself
        ret, out = self.shell.find_prompt ()

        if  ret != 0 :
            self._logger.error ("failed to cancel (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

        lines = filter (None, out.split ("\n"))

        if  len (lines) < 2 :
            self._logger.error ("Cannot evaluate status of bulk job cancel: (%s)(%s)" % (ret, out))
            return

        if lines[-2] != "OK" :
            self._logger.error ("failed to cancel (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_get_states (self, jobs) :

        self._logger.debug ("container get_state: %s"  %  str(jobs))

        bulk   = "BULK\n"
        states = []

        for job in jobs :
            rm, pid = self._adaptor.parse_id (job.id)
            bulk   += "STATE %s\n" % pid

        bulk += "BULK_RUN\n"
        self.shell.run_async (bulk)

        for job in jobs :

            ret, out = self.shell.find_prompt ()

            if  ret != 0 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to get job state: (%s)(%s)" % (ret, out))
                continue

            lines = filter (None, out.split ("\n"))

            if  len (lines) < 2 :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to get job state : (%s)(%s)" % (ret, out))
                continue

            if lines[-2] != "OK" :
                job._adaptor._state     = saga.job.FAILED
                job._adaptor._exception = saga.NoSuccess ("failed to get job state : (%s)(%s)" % (ret, out))
                continue

            state = self._adaptor.string_to_state (lines[-1])

            job._adaptor._state = state
            states.append (state)


        # we also need to find the output of the bulk op itself
        ret, out = self.shell.find_prompt ()

        if  ret != 0 :
            self._logger.error ("failed to get state for (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

        lines = filter (None, out.split ("\n"))

        if  len (lines) < 2 :
            self._logger.error ("Cannot evaluate status of bulk job get_state: (%s)(%s)" % (ret, out))
            return

        if lines[-2] != "OK" :
            self._logger.error ("failed to get state for (parts of the) bulk jobs: (%s)(%s)" % (ret, out))
            return

        return states


###############################################################################
#
class ShellJob (saga.adaptors.cpi.job.Job) :
    """ Implements saga.adaptors.cpi.job.Job
    """
    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (ShellJob, self)
        self._cpi_base.__init__ (api, adaptor)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, job_info):
        """ Implements saga.adaptors.cpi.job.Job.init_instance()
        """

        if  'job_description' in job_info :
            # comes from job.service.create_job()
            self.js = job_info["job_service"] 
            self.jd = job_info["job_description"]

            # the js is responsible for job bulk operations -- which
            # for jobs only work for run()
            self._container       = self.js
            self._method_type     = "run"

            # initialize job attribute values
            self._id              = None
            self._state           = saga.job.NEW
            self._exit_code       = None
            self._exception       = None
            self._created         = time.time ()
            self._started         = None
            self._finished        = None

        elif 'job_id' in job_info :
            # initialize job attribute values
            self.js               = job_info["job_service"] 
            self._id              = job_info['job_id']
            self._state           = saga.job.UNKNOWN
            self._exit_code       = None
            self._exception       = None
            self._created         = None
            self._started         = None
            self._finished        = None

        else :
            # don't know what to do...
            raise saga.BadParameter ("Cannot create job, insufficient information")
        
        return self.get_api ()


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_description (self):
        return self.jd


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state (self):
        """ Implements saga.adaptors.cpi.job.Job.get_state() """

        # may not yet have backend representation, state is probably 'NEW'
        if self._id == None :
            return self._state

        # no need to re-fetch final states
        if  self._state == saga.job.DONE      or \
            self._state == saga.job.FAILED    or \
            self._state == saga.job.CANCELED     :
                return self._state

        stats = self.js._job_get_stats (self._id)

        if 'start' in stats : self._started  = stats['start']
        if 'stop'  in stats : self._finished = stats['stop']
        
        if  not 'state' in stats :
            raise saga.NoSuccess ("failed to get job state for '%s': (%s)" % id)

        self._state = self._adaptor.string_to_state (stats['state'])

        return self._state


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created (self) : 

        # no need to refresh stats -- this is set locally
        return self._created


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started (self) : 

        self.get_state () # refresh stats
        return self._started


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished (self) : 

        self.get_state () # refresh stats
        return self._finished


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_manager (self):

        if not self.manager :
            raise saga.IncorrectState ("Resource Manager unknown")
        
        return self.manager


    # ----------------------------------------------------------------
    #
    # TODO: this should also fetch the (final) state, to safe a hop
    # TODO: implement via notifications
    #
    @SYNC_CALL
    def wait (self, timeout):
        """ 
        A call to the shell to do the WAIT would block the shell for any
        other interactions.  In particular, it would practically kill it if the
        Wait waits forever...

        So we implement the wait via a state pull.  The *real* solution is, of
        course, to implement state notifications, and wait for such
        a notification to arrive within timeout seconds...
        """

        time_start = time.time ()
        time_now   = time_start

        while True :

            state = self.get_state ()

            if  state == saga.job.DONE      or \
                state == saga.job.FAILED    or \
                state == saga.job.CANCELED     :
                    return True

            # avoid busy poll
            # FIXME: self-tune by checking call latency
            time.sleep (0.5)

            # check if we hit timeout
            if  timeout >= 0 :
                time_now = time.time ()
                if  time_now - time_start > timeout :
                    return False
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id (self) :
        """ Implements saga.adaptors.cpi.job.Job.get_id() """        
        return self._id
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code (self) :
        """ Implements saga.adaptors.cpi.job.Job.get_exit_code() """

        if self._exit_code != None :
            return self._exit_code

        self._exit_code = self.js._job_get_exit_code (self._id)

        return self._exit_code
   
    # ----------------------------------------------------------------
    #
    # TODO: the values below should be fetched with every get_state...
    #
    @SYNC_CALL
    def get_execution_hosts (self) :
        """ Implements saga.adaptors.cpi.job.Job.get_execution_hosts()
        """        
        self._logger.debug ("this is the shell adaptor, reporting execution hosts")
        return [self.js.get_url ().host]
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def run (self): 
        self._id = self.js._job_run (self.jd)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def suspend (self):
        self.js._job_suspend (self._id)
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def resume (self):
        self.js._job_resume (self._id)
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel (self, timeout):
        self.js._job_cancel (self._id)
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def re_raise (self):
        # nothing to do here actually, as run () is synchronous...
        return self._exception


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

