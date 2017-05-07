
__author__    = "Andre Merzky, Ashley Z, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" SLURM job adaptor implementation """

#TODO: Throw errors if a user does not specify the MINIMUM number of
#      attributes required for SLURM in a job description

import saga.utils.pty_shell

import saga.url as surl
import saga.adaptors.base
import saga.adaptors.cpi.job

import re
import os
import time
import textwrap
import string
import tempfile

SYNC_CALL  = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL

# ------------------------------------------------------------------------------
#
def log_error_and_raise(message, exception, logger):
    logger.error(message)
    raise exception(message)

# ------------------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# ------------------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "saga.adaptor.slurm_job"
_ADAPTOR_SCHEMAS       = ["slurm", "slurm+ssh", "slurm+gsissh"]
_ADAPTOR_OPTIONS       = []

# ------------------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
# TODO: FILL ALL IN FOR SLURM
_ADAPTOR_CAPABILITIES  = {
    "jdes_attributes"  : [saga.job.NAME,
                          saga.job.EXECUTABLE,
                          saga.job.PRE_EXEC,
                          saga.job.POST_EXEC,
                          saga.job.ARGUMENTS,
                          saga.job.ENVIRONMENT,
                          saga.job.SPMD_VARIATION, #implement later, somehow
                          saga.job.TOTAL_CPU_COUNT,
                          saga.job.NUMBER_OF_PROCESSES,
                          saga.job.PROCESSES_PER_HOST,
                          saga.job.THREADS_PER_PROCESS,
                          saga.job.WORKING_DIRECTORY,
                          #saga.job.INTERACTIVE,
                          saga.job.INPUT,
                          saga.job.OUTPUT,
                          saga.job.ERROR,
                          saga.job.FILE_TRANSFER,
                          saga.job.CLEANUP,
                          saga.job.JOB_START_TIME,
                          saga.job.WALL_TIME_LIMIT,
                          saga.job.TOTAL_PHYSICAL_MEMORY,
                          #saga.job.CPU_ARCHITECTURE,
                          #saga.job.OPERATING_SYSTEM_TYPE,
                          saga.job.CANDIDATE_HOSTS,
                          saga.job.QUEUE,
                          saga.job.PROJECT,
                          saga.job.JOB_CONTACT],

    "job_attributes"   : [saga.job.EXIT_CODE,
                          saga.job.EXECUTION_HOSTS,
                          saga.job.CREATED,
                          saga.job.STARTED,
                          saga.job.FINISHED],
    "metrics"          : [saga.job.STATE,
                          saga.job.STATE_DETAIL],
    "contexts"         : {"ssh"      : "public/private keypair",
                          "x509"     : "X509 proxy for gsissh",
                          "userpass" : "username/password pair for simple ssh"}
}

# ------------------------------------------------------------------------------
# the adaptor documentation
#

        # General Notes
        # *************

        # On Stampede, returning a non-zero exit code results in the scheduler
        # putting the job into a FAILED state and assigning it an exit code of 127.

        # **Example:**

        # ::

        #   js = saga.job.Service("slurm+ssh://stampede")
        #   jd.executable  = '/bin/exit'
        #   jd.arguments   = ['3']
        #   job = js.create_job(jd)
        #   job.run()

        # Will return something similar to (personal account information
        # removed)::

        #   (saga-python-env)ashleyz@login1:~$ scontrol show job 309684
        #   JobId=309684 Name=SlurmJob
        #      UserId=_____ GroupId=__________
        #      Priority=3000 Account=_____ QOS=normal
        #      JobState=FAILED Reason=NonZeroExitCode Dependency=(null)
        #      Requeue=0 Restarts=0 BatchFlag=1 ExitCode=127:0
        #      RunTime=00:00:05 TimeLimit=00:01:00 TimeMin=N/A
        #      SubmitTime=2013-02-22T20:26:50 EligibleTime=2013-02-22T20:26:50
        #      StartTime=2013-02-22T20:26:50 EndTime=2013-02-22T20:26:55
        #      PreemptTime=None SuspendTime=None SecsPreSuspend=0
        #      Partition=development AllocNode:Sid=login1:12070
        #      ReqNodeList=(null) ExcNodeList=(null)
        #      NodeList=c557-401
        #      BatchHost=c557-401
        #      NumNodes=1 NumCPUs=16 CPUs/Task=1 ReqS:C:T=*:*:*
        #      MinCPUsNode=1 MinMemoryNode=0 MinTmpDiskNode=0
        #      Features=(null) Gres=(null) Reservation=(null)
        #      Shared=0 Contiguous=0 Licenses=(null) Network=(null)
        #      Command=/home1/01414/_______/.saga/adaptors/slurm_job/wrapper.sh
        #      WorkDir=/home1/01414/_______/

        # I'm not sure how to fix this for the time being.

        # Suspend/resume do not appear to be supported for regular
        # users on Stampede.

        # run_job is not supported, as there are many attributed (queues,
        # projects, etc) which need to be passed to the adaptor.  I could
        # add URL parsing so that you could pile on queue/project/jobname
        # information if this has any strong usecases, but I avoided doing
        # so for now to decrease complexity/possible confusion.

        # Cancelling a job with scontrol, puts it into a COMPLETING state, which
        # is parsed by the SLURM status parser as saga.job.RUNNING (see the
        # SLURM docs, COMPLETING is a state a job goes into when it is done
        # running but still flushing IO/etc).  Anyhow, I put some code in to
        # manually put the job into CANCELED state when the job is canceled,
        # but I'm not sure that this is reported correctly everywhere yet.

        # What exit code should be returned for a CANCELED job?


_ADAPTOR_DOC           = {
    "name"             : _ADAPTOR_NAME,
    "cfg_options"      : _ADAPTOR_OPTIONS,
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "description"      : """
        The SLURM adaptor allows to run and manage jobs on a
        `SLURM <https://computing.llnl.gov/linux/slurm/slurm.html>`_ HPC cluster.

        Implementation Notes
        ********************

         - If scontrol can't find an exit code, it returns None
           (see _job_get_exit_code)
         - If scancel can't cancel a job, we raise an exception
           (see _job_cancel)
         - If we can't suspend a job with scontrol suspend, we raise an exception
           (see _job_suspend).  scontrol suspend NOT supported on Stampede
         - I started to implement a dictionary to keep track of jobs locally.
           It works to the point where the unit tests are passed, but I have
           not gone over theis extensively...
         - Relating to the above, _job_get_info is written, but unused/untested
           (mostly from PBS adaptor)

        """,
    "example": "examples/jobs/slurmjob.py",
    "schemas": {"slurm":        "connect to a local cluster",
                "slurm+ssh":    "conenct to a remote cluster via SSH",
                "slurm+gsissh": "connect to a remote cluster via GSISSH"}
}

# ------------------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    "name"             : _ADAPTOR_NAME,
    "version"          : "v0.2",
    "schemas"          : _ADAPTOR_SCHEMAS,
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "cpis"             : [
        {
        "type"         : "saga.job.Service",
        "class"        : "SLURMJobService"
        },
        {
        "type"         : "saga.job.Job",
        "class"        : "SLURMJob"
        }
    ]
}

###############################################################################
# The adaptor class

class Adaptor (saga.adaptors.base.Base):
    """
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """


    # --------------------------------------------------------------------------
    #
    def __init__ (self) :

        saga.adaptors.base.Base.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile ('^\[(.*)\]-\[(.*?)\]$')

    # --------------------------------------------------------------------------
    #
    def sanity_check (self) :
        pass


    def parse_id (self, id) :
        # split the id '[rm]-[pid]' in its parts, and return them.

        match = self.id_re.match (id)

        if  not match or len (match.groups()) != 2 :
            raise saga.BadParameter ("Cannot parse job id '%s'" % id)

        return (match.group(1), match.group (2))


###############################################################################
#
class SLURMJobService (saga.adaptors.cpi.job.Service) :
    """ Implements saga.adaptors.cpi.job.Service """

    # --------------------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        _cpi_base = super  (SLURMJobService, self)
        _cpi_base.__init__ (api, adaptor)

        # TODO make sure this formats properly and works right!
        self.exit_code_re            = re.compile(r"\bExitCode  \b=(\d*)", re.VERBOSE)
        self.scontrol_jobstate_re    = re.compile(r"\bJobState  \b=(\S*)", re.VERBOSE)
        self.scontrol_job_name_re    = re.compile(r"\bJobName   \b=(\S*)", re.VERBOSE)
        self.scontrol_create_time_re = re.compile(r"\bSubmitTime\b=(\S*)", re.VERBOSE)
        self.scontrol_start_time_re  = re.compile(r"\bStartTime \b=(\S*)", re.VERBOSE)
        self.scontrol_end_time_re    = re.compile(r"\bEndTime   \b=(\S*)", re.VERBOSE)
        self.scontrol_comp_time_re   = re.compile(r"\bRunTime   \b=(\S*)", re.VERBOSE)
        self.scontrol_exec_hosts_re  = re.compile(r"\bNodeList  \b=(\S*)", re.VERBOSE)

        # these are the commands that we need in order to interact with SLURM
        # the adaptor will try to find them when it first opens the shell
        # connection, and bails out in case they are not available.
        self._commands = {'sbatch': None,
                          'squeue': None,
                          'scontrol': None,
                          'scancel': None}

    # --------------------------------------------------------------------------
    #
    def __del__ (self) :
        try :
            if self.shell : del (self.shell)
        except :
            pass


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, rm_url, session) :
        """ Service instance constructor """

        self.rm      = rm_url
        self.session = session

        self.jobs = {}
        self._open ()

        return self.get_api ()


    # --------------------------------------------------------------------------
    #
    def close (self) :
        if  self.shell :
            self.shell.finalize (True)


    # --------------------------------------------------------------------------
    #
    def _open (self) :
        """
        Open our persistent shell for this job adaptor.  We use
        the pty_shell functionality for this.
        """
        # check to see what kind of connection we will want to create
        if self.rm.schema   == "slurm":
            shell_schema = "fork://"
        elif self.rm.schema == "slurm+ssh":
            shell_schema = "ssh://"
        elif self.rm.schema == "slurm+gsissh":
            shell_schema = "gsissh://"
        else:
            raise saga.IncorrectURL("Schema %s not supported by SLURM adaptor."
                                    % self.rm.schema)

        #<scheme>://<user>:<pass>@<host>:<port>/<path>?<query>#<fragment>
        # build our shell URL
        shell_url = shell_schema

        # did we provide a username and password?
        if self.rm.username and self.rm.password:
            shell_url += self.rm.username + ":" + self.rm.password + "@"

        # only provided a username
        if self.rm.username and not self.rm.password:
            shell_url += self.rm.username + "@"

        #add hostname
        shell_url += self.rm.host

        #add port
        if  self.rm.port:
            shell_url += ":" + str(self.rm.port)

        shell_url = saga.url.Url(shell_url)

        # establish shell connection
        self._logger.debug("Opening shell of type: %s" % shell_url)
        self.shell = saga.utils.pty_shell.PTYShell (shell_url,
                                                    self.session,
                                                    self._logger)

        # verify our SLURM environment contains the commands we need for this
        # adaptor to work properly
        self._logger.debug("Verifying existence of remote SLURM tools.")
        for cmd in self._commands.keys():
            ret, out, _ = self.shell.run_sync("which %s " % cmd)
            if ret != 0:
                message = "Error finding SLURM tool %s on remote server %s!\n" \
                          "Locations searched:\n%s\n" \
                          "Is SLURM installed on that machine? " \
                          "If so, is your remote SLURM environment "\
                          "configured properly? " % (cmd, self.rm, out)
                raise saga.NoSuccess._log (self._logger, message)

        self._logger.debug ("got cmd prompt (%s)(%s)" % (ret, out))

        self.rm.detected_username = self.rm.username
        # figure out username if it wasn't made explicit
        # important if .ssh/config info read+connected with
        # a different username than what we expect
        if not self.rm.username:
            self._logger.debug ("No username provided in URL %s, so we are"
                                " going to find it with whoami" % self.rm)
            ret, out, _ = self.shell.run_sync("whoami")
            self.rm.detected_username = out.strip()
            self._logger.debug("Username detected as: %s",
                               self.rm.detected_username)

        return

    # --------------------------------------------------------------------------
    #
    def _close (self) :
        """
        Close our shell connection
        """
        del (self.shell)
        self.shell = None


    # --------------------------------------------------------------------------
    #
    #
    def _job_run (self, jd) :
        """ runs a job on the wrapper via pty, and returns the job id """

        #define a bunch of default args
        exe                 = jd.executable
        pre                 = jd.as_dict().get(saga.job.PRE_EXEC)
        post                = jd.as_dict().get(saga.job.POST_EXEC)
        args                = jd.as_dict().get(saga.job.ARGUMENTS, [])
        env                 = jd.as_dict().get(saga.job.ENVIRONMENT, dict())
        cwd                 = jd.as_dict().get(saga.job.WORKING_DIRECTORY)
        job_name            = jd.as_dict().get(saga.job.NAME)
        spmd_variation      = jd.as_dict().get(saga.job.SPMD_VARIATION)
        total_cpu_count     = jd.as_dict().get(saga.job.TOTAL_CPU_COUNT)
        number_of_processes = jd.as_dict().get(saga.job.NUMBER_OF_PROCESSES)
        processes_per_host  = jd.as_dict().get(saga.job.PROCESSES_PER_HOST)
        output              = jd.as_dict().get(saga.job.OUTPUT, "radical.saga.default.out")
        error               = jd.as_dict().get(saga.job.ERROR)
        file_transfer       = jd.as_dict().get(saga.job.FILE_TRANSFER)
        wall_time_limit     = jd.as_dict().get(saga.job.WALL_TIME_LIMIT)
        queue               = jd.as_dict().get(saga.job.QUEUE)
        project             = jd.as_dict().get(saga.job.PROJECT)
        job_memory          = jd.as_dict().get(saga.job.TOTAL_PHYSICAL_MEMORY)
        job_contact         = jd.as_dict().get(saga.job.JOB_CONTACT)
        candidate_hosts     = jd.as_dict().get(saga.job.CANDIDATE_HOSTS)

        # check to see what's available in our job description
        # to override defaults

        # try to create the working directory (if defined)
        # NOTE: this assumes a shared filesystem between login node and
        #       comnpute nodes.
        if cwd:
             self._logger.info("Creating working directory %s" % cwd)
             ret, out, _ = self.shell.run_sync("mkdir -p %s"   % cwd)
             if ret:
                 # something went wrong
                 message = "Couldn't create working directory - %s" % (out)
                 log_error_and_raise(message, saga.NoSuccess, self._logger)


        if isinstance(candidate_hosts, list):
            candidate_hosts = ','.join(candidate_hosts)

        if isinstance(job_contact, list):
            job_contact = job_contact[0]

        if project and ':' in project:
            account, reservation = project.split()
        else:
            account, reservation = project, None

        slurm_script = "#!/bin/sh\n\n"

        # make sure we have something for total_cpu_count
        if not total_cpu_count:
            total_cpu_count = 1

        # make sure we have something for number_of_processes
        if not number_of_processes:
            number_of_processes = total_cpu_count

        if spmd_variation:
            if spmd_variation.lower() not in 'mpi':
                raise saga.BadParameter("Slurm cannot handle spmd variation '%s'" % spmd_variation)
            mpi_cmd = 'mpirun -n %d ' % number_of_processes

        else:
            # we start N independent processes
            mpi_cmd = ''
            slurm_script += "#SBATCH --ntasks=%s\n" % (number_of_processes)

            if total_cpu_count and number_of_processes:
                slurm_script += "#SBATCH --cpus-per-task=%s\n" \
                              % (int(total_cpu_count / number_of_processes))

            if processes_per_host:
                slurm_script += "#SBATCH --ntasks-per-node=%s\n" % processes_per_host

        if cwd:             slurm_script += "#SBATCH --workdir %s\n"     % cwd 
        if output:          slurm_script += "#SBATCH --output %s\n"      % output 
        if error:           slurm_script += "#SBATCH --error %s\n"       % error 
        if queue:           slurm_script += "#SBATCH --partition %s\n"   % queue
        if job_name:        slurm_script += '#SBATCH -J "%s"\n'          % job_name
        if job_memory:      slurm_script += "#SBATCH --mem=%s\n"         % job_memory 
        if candidate_hosts: slurm_script += "#SBATCH --nodelist=%s\n"    % candidate_hosts 
        if job_contact:     slurm_script += "#SBATCH --mail-user=%s\n"   % job_contact
        if account:         slurm_script += "#SBATCH --account %s\n"     % account
        if reservation:     slurm_script += "#SBATCH --reservation %s\n" % reservation
        if wall_time_limit: slurm_script += "#SBATCH --time %02d:%02d:00\n" \
                                          % (wall_time_limit/60,wall_time_limit%60)
        if env:
            slurm_script += "\n## ENVIRONMENT\n"
            for key,val in env.iteritems():
                slurm_script += 'export "%s"="%s"\n'  %  (key, val)

        if pre:
            slurm_script += "\n## PRE_EXEC\n" + "\n".join(pre)
            slurm_script += '\n'

        # create our commandline
        slurm_script += "\n## EXEC\n"
        slurm_script += '%s%s %s' % (mpi_cmd, exe, ' '.join(args))
        slurm_script += '\n'

        if post :
            slurm_script += "\n## POST_EXEC\n" + '\n'.join(post)
            slurm_script += '\n'

        # write script into a tmp file for staging
        self._logger.info ("SLURM script generated:\n%s" % slurm_script)

        tgt = os.path.basename (tempfile.mktemp (suffix='.slurm', prefix='tmp_'))
        self.shell.write_to_remote (src=slurm_script, tgt=tgt)

        # submit the job
        ret, out, _ = self.shell.run_sync ("sbatch '%s'; rm -vf '%s'" % (tgt, tgt))

        self._logger.debug ("staged/submit SLURM script (%s) (%s)" % (tgt, ret))

        # find out what our job ID is
        # TODO: Could make this more efficient
        self.job_id = None
        for line in out.split("\n"):
            if "Submitted batch job" in line:
                self.job_id = "[%s]-[%s]" % \
                    (self.rm, int(line.split()[-1:][0]))
                break

        # if we have no job ID, there's a failure...
        if not self.job_id:
            raise saga.NoSuccess._log(self._logger,
                             "Couldn't get job id from submitted job!"
                              " sbatch output:\n%s" % out)

        self._logger.debug("started job %s" % self.job_id)
        self._logger.debug("Batch system output:\n%s" % out)

        # create local jobs dictionary entry
        self.jobs[self.job_id] = {
                'state'      : saga.job.PENDING,
                'create_time': None,
                'start_time' : None,
                'end_time'   : None,
                'comp_time'  : None,
                'exec_hosts' : None,
                'gone'       : False
            }

        return self.job_id


    # --------------------------------------------------------------------------
    #
    # FROM STAMPEDE'S SQUEUE MAN PAGE
    #
    # JOB STATE CODES
    #    Jobs typically pass through several states in the course of their
    #    execution.  The typical states are PENDING, RUNNING, SUSPENDED,
    #    COMPLETING, and COMPLETED.   An  explanation  of  each state follows.
    #
    #    CA  CANCELED        Job was explicitly cancelled by the user or system
    #                        administrator.  The job may or may not have been
    #                        initiated.
    #    CD  COMPLETED       Job has terminated all processes on all nodes.
    #    CF  CONFIGURING     Job has been allocated resources, but are waiting
    #                        for them to become ready for use (e.g. booting).
    #    CG  COMPLETING      Job is in the process of completing. Some processes
    #                        on some nodes may still be active.
    #    F   FAILED          Job terminated with non-zero exit code or other
    #                        failure condition.
    #    NF  NODE_FAIL       Job terminated due to failure of one or more
    #                        allocated nodes.
    #    PD  PENDING         Job is awaiting resource allocation.
    #    PR  PREEMPTED       Job terminated due to preemption.
    #    R   RUNNING         Job currently has an allocation.
    #    S   SUSPENDED       Job has an allocation, but execution has been
    #                        suspended.
    #    TO  TIMEOUT         Job terminated upon reaching its time limit.
    #
    def _slurm_to_saga_jobstate(self, slurmjs):
        """
        translates a slurm one-letter state to saga
        """

        if   slurmjs in ['CA', "CANCELLED"  ]: return saga.job.CANCELED
        elif slurmjs in ['CD', "COMPLETED"  ]: return saga.job.DONE
        elif slurmjs in ['CF', "CONFIGURING"]: return saga.job.PENDING
        elif slurmjs in ['CG', "COMPLETING" ]: return saga.job.RUNNING
        elif slurmjs in ['F' , "FAILED"     ]: return saga.job.FAILED
        elif slurmjs in ['NF', "NODE_FAIL"  ]: return saga.job.FAILED
        elif slurmjs in ['PD', "PENDING"    ]: return saga.job.PENDING
        elif slurmjs in ['PR', "PREEMPTED"  ]: return saga.job.CANCELED
        elif slurmjs in ['R' , "RUNNING"    ]: return saga.job.RUNNING
        elif slurmjs in ['S' , "SUSPENDED"  ]: return saga.job.SUSPENDED
        elif slurmjs in ['TO', "TIMEOUT"    ]: return saga.job.CANCELED
        else                                 : return saga.job.UNKNOWN


    # --------------------------------------------------------------------------
    #
    def _job_cancel (self, job):
        """
        Given a job id, attempt to cancel it through use of commandline
        scancel.  Raises exception when unsuccessful.
        """

        if job._state in [saga.job.DONE, saga.job.FAILED, saga.job.CANCELED]:
            # job is already final - nothing to do
            return

        if job._state in [saga.job.NEW]:
            # job is not yet submitted - nothing to do
            job._state = saga.job.CANCELED

        if not job._id:
            # uh oh - what to do?
            raise saga.NoSuccess._log(self._logger,
                    "Could not cancel job: no job ID")

        rm,  pid    = self._adaptor.parse_id(job._id)
        ret, out, _ = self.shell.run_sync("scancel %s" % pid)

        if ret != 0:
            raise saga.NoSuccess._log(self._logger,
                    "Could not cancel job %s because: %s" % (pid, out))

        job._state = saga.job.CANCELED


    # --------------------------------------------------------------------------
    #
    def _job_suspend (self, job):
        """
        Attempt to suspend a job with commandline scontrol.  Raise
        exception when unsuccessful.
        """

        if job._state in [saga.job.DONE,     saga.job.FAILED,
                          saga.job.CANCELED, saga.job.NEW,
                          saga.job.SUSPENDED]:
            raise saga.IncorrectState._log(self._logger,
                    "Could not suspend job %s in state %s" % (job._id, job._state))


        rm,  pid    = self._adaptor.parse_id (job._id)
        ret, out, _ = self.shell.run_sync("scontrol suspend %s" % pid)

        if ret == 0:
            return True

        # check to see if the error was a permission error
        elif "Access/permission denied" in out:
            raise saga.PermissionDenied._log(self._logger,
                                      "Could not suspend job %s because: %s" % (pid, out))

        # it's some other error
        else:
            raise saga.NoSuccess._log(self._logger,
                                      "Could not suspend job %s because: %s" % (pid, out))

    # --------------------------------------------------------------------------
    #
    def _job_resume (self, job):
        """
        Attempt to resume a job with commandline scontrol.  Raise
        exception when unsuccessful.
        """

        if job._state in [saga.job.DONE,     saga.job.FAILED,
                          saga.job.CANCELED, saga.job.NEW,
                          saga.job.RUNNING]:
            raise saga.IncorrectState._log(self._logger,
                    "Could not resume job %s in state %s" % (job._id, job._state))


        rm,  pid    = self._adaptor.parse_id (job._id)
        ret, out, _ = self.shell.run_sync("scontrol resume %s" % pid)

        if ret == 0:
            return True

        # check to see if the error was a permission error
        elif "Access/permission denied" in out:
            raise saga.PermissionDenied._log(self._logger,
                                      "Could not suspend job %s because: %s" % (pid, out))

        # it's some other error
        else:
            raise saga.NoSuccess._log(self._logger,
                                      "Could not resume job %s because: %s" % (pid, out))


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def create_job (self, jd) :
        """ Implements saga.adaptors.cpi.job.Service.create_job()
        """

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = { "job_service"     : self,
                          "job_description" : jd,
                          "job_schema"      : self.rm.schema,
                          "reconnect"       : False}

        return saga.job.Job (_adaptor=self._adaptor,
                             _adaptor_state=adaptor_state)

    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url (self) :
        """ Implements saga.adaptors.cpi.job.Service.get_url()
        """
        return self.rm


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self):
        """ Implements saga.adaptors.cpi.job.Service.list()
        """

        # ashleyz@login1:~$ squeue -h -o "%i" -u ashleyz
        # 255042
        # 255035
        # 255028
        # 255018

        # this line gives us a nothing but jobids for our user
        ret, out, _ = self.shell.run_sync('squeue -h -o "%%i" -u %s'
                                          % self.rm.detected_username)

        # mangle our results into the proper id format
        output = ["[%s]-[%s]" % (self.rm, i) for i in out.strip().split("\n")]
        return output


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job (self, jobid):

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.  The job adaptor will run 'scontrol
        # show job $jobid' to complement the information.
        adaptor_state = {"job_service"    : self,
                         "job_description": saga.job.Description(),
                         "job_schema"     : self.rm.schema,
                         "reconnect"      : True,
                         "reconnect_jobid": jobid
                        }
        return saga.job.Job(_adaptor=self._adaptor,
                            _adaptor_state=adaptor_state)
        


    # --------------------------------------------------------------------------
    #
    def container_run(self, jobs):

        for job in jobs:
            job.run()


    # --------------------------------------------------------------------------
    #
    def container_wait(self, jobs, mode, timeout):

        # TODO: this is not optimized yet
        for job in jobs:
            job.wait()


    # --------------------------------------------------------------------------
    #
    def container_cancel(self, jobs, timeout):

        # TODO: this is not optimized yet
        for job in jobs:
            job.cancel(timeout)


    # --------------------------------------------------------------------------
    #
    def container_get_states(self, jobs):

        # TODO: this is not optimized yet
        states = list()
        for job in jobs:
            states.append(job.get_state())
        return states



# ------------------------------------------------------------------------------
#
class SLURMJob(saga.adaptors.cpi.job.Job):
    """
    Implements saga.adaptors.cpi.job.Job
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        _cpi_base = super (SLURMJob, self)
        _cpi_base.__init__(api, adaptor)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, job_info):
        """
        Implements saga.adaptors.cpi.job.Job.init_instance()
        """

        self.jd = job_info["job_description"]
        self.js = job_info["job_service"]

        # the js is responsible for job bulk operations -- which
        # for jobs only work for run()
        self._container       = self.js
        self._method_type     = "run"

        # initialize job attribute values
        self._id              = None
        self._name            = self.jd.as_dict().get(saga.job.NAME, 'saga')
        self._state           = saga.job.NEW
        self._exit_code       = None
        self._exception       = None
        self._started         = None
        self._finished        = None

        # think "reconnect" in terms of "reloading" job id, _NOT_
        # physically creating a new network connection
        if job_info['reconnect']:
            self._id   = job_info['reconnect_jobid']
            other_info = self._job_get_info()
            self._name = other_info.get('job_name')
            self._started = True
        else:
            self._started = False

        return self.get_api ()


    # --------------------------------------------------------------------------
    #
    def _job_get_info (self):
        """
        use scontrol to grab job info
        NOT CURRENTLY USED/TESTED, here for later
        """

        # prev. info contains the info collect when _job_get_info
        # was called the last time
        prev_info = self.js.jobs.get(self._id)

        # if the 'gone' flag is set, there's no need to query the job
        # state again. it's gone forever
        if prev_info:
            if prev_info.get('gone', False):
                self._logger.debug("Job is gone.")
                return prev_info

        # curr. info will contain the new job info collect. it starts off
        # as a copy of prev_info (don't use deepcopy because there is an API
        # object in the dict -> recursion)
        curr_info = dict()
        
        if prev_info:
            curr_info['job_id'     ] = prev_info.get('job_id'     )
            curr_info['job_name'   ] = prev_info.get('job_name'   )
            curr_info['state'      ] = prev_info.get('state'      )
            curr_info['create_time'] = prev_info.get('create_time')
            curr_info['start_time' ] = prev_info.get('start_time' )
            curr_info['end_time'   ] = prev_info.get('end_time'   )
            curr_info['comp_time'  ] = prev_info.get('comp_time'  )
            curr_info['exec_hosts' ] = prev_info.get('exec_hosts' )
            curr_info['gone'       ] = prev_info.get('gone'       )

        rm, pid = self._adaptor.parse_id(self._id)

        # update current info with scontrol
        ret, out, _ = self.js.shell.run_sync('scontrol show job %s' % pid)
      # self._logger.debug("Updating job status using the following information:\n%s" % out)

        # out is comprised of a set of space-limited words like this:
        #
        # ----------------------------------------------------------------------
        # $ scontrol show job 8101313
        #    JobId=8101313 JobName=pilot.0000 UserId=tg803521(803521)
        #    GroupId=G-81625(81625) Priority=1701 Nice=0 Account=TG-MCB090174
        #    QOS=normal JobState=RUNNING Reason=None Dependency=(null) Requeue=0
        #    Restarts=0 BatchFlag=1 Reboot=0 ExitCode=0:0 RunTime=00:00:25
        #    TimeLimit=00:15:00 TimeMin=N/A SubmitTime=2017-01-11T15:47:19
        #    EligibleTime=2017-01-11T15:47:19 StartTime=2017-01-11T15:47:19
        #    EndTime=2017-01-11T16:02:19 PreemptTime=None SuspendTime=None
        #    SecsPreSuspend=0 Partition=development AllocNode:Sid=login3:2886
        #    ReqNodeList=(null) ExcNodeList=(null) NodeList=c557-[901-904]
        #    BatchHost=c557-901 NumNodes=4 NumCPUs=64 CPUs/Task=1
        #    ReqB:S:C:T=0:0:*:* TRES=cpu=64,node=4 Socks/Node=*
        #    NtasksPerN:B:S:C=0:0:*:* CoreSpec=* MinCPUsNode=1 MinMemoryNode=0
        #    MinTmpDiskNode=0 Features=(null) Gres=(null) Reservation=(null)
        #    Shared=0 Contiguous=0 Licenses=(null) Network=(null)
        #    Command=/home1/01083/tg803521/tmp_egGk1n.slurm
        #    WorkDir=/work/01083/
        #    StdIn=/dev/null
        #    StdOut=/work/01083/bootstrap_1.out
        #    StdErr=/work/01083/bootstrap_1.err
        #    Power= SICP=0
        # ----------------------------------------------------------------------
        #
        # so we split on spaces and newlines, and then on '=' to get
        # key-value-pairs.

        elems = out.split()
        data  = dict()
        for elem in elems:

            parts = elem.split('=', 1)

            if len(parts) == 1:
                # default if no '=' is found
                parts.append(None)

            # ignore non-splittable ones
            key, val = parts
            if val in ['', '(null)']:
                val = None
            data[key] = val

        # update state
        if data.get('JobState'):
            curr_info['state'] = self.js._slurm_to_saga_jobstate(data['JobState'])
        else:
            curr_info['state'] = self._job_get_state(self._id)

        # update exit code
        if data.get('ExitCode'):
            curr_info['exit_code'] = data['ExitCode'].split(':')[0]
        else:
            curr_info['exit_code'] = self._job_get_state(self._id)

        curr_info['job_name'   ] = data.get('JobName')
        curr_info['create_time'] = data.get('SubmitTime')
        curr_info['start_time' ] = data.get('StartTime')
        curr_info['end_time'   ] = data.get('EndTime')
        curr_info['comp_time'  ] = data.get('RunTime')
        curr_info['exec_hosts' ] = data.get('NodeList')

        return curr_info


    # --------------------------------------------------------------------------
    #
    def _job_get_state (self, job_id) :
        """ get the job state from the wrapper shell """

        # if the state is NEW and we haven't sent out a run command, keep
        # it listed as NEW
        if self._state == saga.job.NEW and not self._started:
            return saga.job.NEW

        # if the state is DONE, CANCELED or FAILED, it is considered
        # final and we don't need to query the backend again
        if self._state in [saga.job.CANCELED, saga.job.FAILED, saga.job.DONE]:
            return self._state

        rm, pid = self._adaptor.parse_id (job_id)

        try:
            ret, out, _ = self.js.shell.run_sync('scontrol show job %s' % pid)
            match       = self.js.scontrol_jobstate_re.search(out)

            if match:
                slurm_state = match.group(1)
            else:
                # no jobstate found from scontrol
                # the job may have finished a while back, use sacct to
                # look at the full slurm history
                slurm_state = self._sacct_jobstate_match(pid)
                if not slurm_state:
                    # no jobstate found in slurm
                    return saga.job.UNKNOWN

            return self.js._slurm_to_saga_jobstate(slurm_state)

        except Exception, ex:
            raise saga.NoSuccess("Error getting the job state for "
                                 "job %s:\n%s"%(pid,ex))

        raise saga.NoSuccess._log (self._logger,
                                   "Internal SLURM adaptor error"
                                   " in _job_get_state")


    # --------------------------------------------------------------------------
    #
    def _sacct_jobstate_match (self, pid):
        """ get the job state from the slurm accounting data """
        ret, sacct_out, _ = self.js.shell.run_sync(
            "sacct --format=JobID,State --parsable2 --noheader --jobs=%s" % pid)
        # output will look like:
        # 500723|COMPLETED
        # 500723.batch|COMPLETED
        # or:
        # 500682|CANCELLED by 900369
        # 500682.batch|CANCELLED

        for line in sacct_out.strip().split('\n'):
            (slurm_id, slurm_state) = line.split('|', 1)
            if slurm_id == pid and slurm_state:
                return slurm_state.split()[0].strip()

        return None


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        """ Implements saga.adaptors.cpi.job.Job.get_state()
        """
        self._state = self._job_get_state (self._id)
        return self._state


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_description (self):
        return self.jd


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_service_url(self):
        """ implements saga.adaptors.cpi.job.Job.get_service_url()
        """
        return self.js.rm



    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def wait(self, timeout):
        time_start = time.time()
        time_now   = time_start
        rm, pid    = self._adaptor.parse_id(self._id)

        while True:
            state = self._job_get_state(self._id)
            self._logger.debug("wait() state for job id %s:%s"%(self._id, state))

            if state == saga.job.UNKNOWN :
                log_error_and_raise("cannot get job state",
                                    saga.IncorrectState, self._logger)

            if state in [saga.job.DONE, saga.job.FAILED, saga.job.CANCELED]:
                return True

            # check if we hit timeout
            if timeout >= 0:
                if time.time() - time_start > timeout:
                    return False

            # avoid busy poll
            time.sleep(0.5)


    # --------------------------------------------------------------------------
    #
    # In general, the job ID is something which is generated by the adaptor or
    # by the backend, and the user should not interpret it.  Two caveats though:
    #
    # (a) The ID MUST remain constant once it is assigned to a job (imagine an
    # application hashes on job ids, for example.
    #
    # (b) the ID SHOULD follow the scheme [service_url]-[backend-id] -- and in
    # that case, we should make sure that the URL part of the ID can be used to
    # create a new job service instance.
    #
    @SYNC_CALL
    def get_id (self) :
        """
        Implements saga.adaptors.cpi.job.Job.get_id()
        """
        return self._id


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_name (self):
        """
        Implements saga.adaptors.cpi.job.Job.get_name()
        """
        if not self._name:
            self._name = self._job_get_info()['job_name']
        return self._name


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code(self) :
        """
        Implements saga.adaptors.cpi.job.Job.get_exit_code()
        """
        # FIXME: use cache
        return self._job_get_info()['exit_code']


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def suspend(self) :
        """
        Implements saga.adaptors.cpi.job.Job.get_exit_code()
        """
        return self.js._job_suspend(self)


    # --------------------------------------------------------------------------
    @SYNC_CALL
    def resume(self) :
        """
        Implements saga.adaptors.cpi.job.Job.get_exit_code()
        """
        return self.js._job_resume(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created(self) :
        """
        Implements saga.adaptors.cpi.job.Job.get_created()
        """
        # FIXME: use cache
        return self._job_get_info()['create_time']


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started(self) :
        """
        Implements saga.adaptors.cpi.job.Job.get_started()
        """
        # FIXME: use cache
        return self._job_get_info()['start_time']


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished(self) :
        """
        Implements saga.adaptors.cpi.job.Job.get_finished()
        """
        # FIXME: use cache
        return self._job_get_info()['end_time']


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_execution_hosts(self) :
        """
        Implements saga.adaptors.cpi.job.Job.get_execution_hosts()
        """
        # FIXME: use cache
        return self._job_get_info()['exec_hosts']


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):
        """
        Implements saga.adaptors.cpi.job.Job.cancel()
        """
        self.js._job_cancel(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):
        """
        Implements saga.adaptors.cpi.job.Job.run()
        """
        self._id      = self.js._job_run(self.jd)
        self._started = True


# ------------------------------------------------------------------------------

