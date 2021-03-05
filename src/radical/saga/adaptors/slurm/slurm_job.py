
__author__    = "Andre Merzky, Ashley Z, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


'''
SLURM job adaptor implementation
'''

import re
import os
import math
import time
import datetime
import tempfile

import radical.utils as ru

from ...job           import constants   as c
from ...utils         import pty_shell   as rsups
from ...              import job         as api_job
from ...              import exceptions  as rse
from ...              import filesystem  as sfs
from ..               import base        as a_base
from ..cpi            import job         as cpi_job
from ..cpi            import decorators  as cpi_decs

from ...utils.job     import TransferDirectives

SYNC_CALL  = cpi_decs.SYNC_CALL
ASYNC_CALL = cpi_decs.ASYNC_CALL


# ------------------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# ------------------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "radical.saga.adaptors.slurm_job"
_ADAPTOR_SCHEMAS       = ["slurm", "slurm+ssh", "slurm+gsissh"]
_ADAPTOR_OPTIONS       = []

# ------------------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
# TODO: FILL ALL IN FOR SLURM
_ADAPTOR_CAPABILITIES  = {
    "jdes_attributes"  : [c.NAME,
                          c.EXECUTABLE,
                          c.PRE_EXEC,
                          c.POST_EXEC,
                          c.ARGUMENTS,
                          c.ENVIRONMENT,
                          c.SPMD_VARIATION,
                          c.TOTAL_CPU_COUNT,
                          c.TOTAL_GPU_COUNT,
                          c.NUMBER_OF_PROCESSES,
                          c.PROCESSES_PER_HOST,
                          c.THREADS_PER_PROCESS,
                          c.WORKING_DIRECTORY,
                        # c.INTERACTIVE,
                          c.INPUT,
                          c.OUTPUT,
                          c.ERROR,
                          c.FILE_TRANSFER,
                          c.CLEANUP,
                          c.WALL_TIME_LIMIT,
                          c.TOTAL_PHYSICAL_MEMORY,
                          c.SYSTEM_ARCHITECTURE,
                        # c.OPERATING_SYSTEM_TYPE,
                          c.CANDIDATE_HOSTS,
                          c.QUEUE,
                          c.PROJECT,
                          c.JOB_CONTACT],
    "job_attributes"   : [c.EXIT_CODE,
                          c.EXECUTION_HOSTS,
                          c.CREATED,
                          c.STARTED,
                          c.FINISHED],
    "metrics"          : [c.STATE,
                          c.STATE_DETAIL],
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
        # putting the job into a FAILED state and assigning it an exit code of
        # 127.

        # **Example:**

        #   js = rs.job.Service("slurm+ssh://stampede")
        #   jd.executable  = '/bin/exit'
        #   jd.arguments   = ['3']
        #   job = js.create_job(jd)
        #   job.run()

        # Will return something similar to (personal account information
        # removed)::

        #   (ve) ashleyz@login1:~$ scontrol show job 309684
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
        # is parsed by the SLURM status parser as rs.job.RUNNING (see the
        # SLURM docs, COMPLETING is a state a job goes into when it is done
        # running but still flushing IO/etc).  Anyhow, I put some code in to
        # manually put the job into CANCELED state when the job is canceled,
        # but I'm not sure that this is reported correctly everywhere yet.

        # What exit code should be returned for a CANCELED job?


_ADAPTOR_DOC           = {
    "name"             : _ADAPTOR_NAME,
    "cfg_options"      : _ADAPTOR_OPTIONS,
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "description"      : '''
        The SLURM adaptor allows to run and manage jobs on a
        `SLURM <https://computing.llnl.gov/linux/slurm/>`_ HPC cluster.

        Implementation Notes
        ********************

         - If scontrol can't find an exit code, it returns None
           (see _job_get_exit_code)
         - If scancel can't cancel a job, we raise an exception
           (see _job_cancel)
         - If we can't suspend a job with scontrol suspend, we raise an
           exception (see _job_suspend).  scontrol suspend NOT supported on
           Stampede.
         - I started to implement a dictionary to keep track of jobs locally.
           It works to the point where the unit tests are passed, but I have
           not gone over theis extensively...
         - Relating to the above, _job_get_info is written, but unused/untested
           (mostly from PBS adaptor)

        ''',
    "example": "examples/jobs/slurmjob.py",
    "schemas": {"slurm":        "connect to a local cluster",
                "slurm+ssh":    "conenct to a remote cluster via SSH",
                "slurm+gsissh": "connect to a remote cluster via GSISSH"}
}

# ------------------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    "name"             : _ADAPTOR_NAME,
    "version"          : "v0.2.1",
    "schemas"          : _ADAPTOR_SCHEMAS,
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "cpis"             : [
        {
            "type"     : "radical.saga.job.Service",
            "class"    : "SLURMJobService"
        },
        {
            "type"     : "radical.saga.job.Job",
            "class"    : "SLURMJob"
        }
    ]
}


################################################################################
#
# The adaptor class
#
class Adaptor(a_base.Base):
    '''
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    '''


    # --------------------------------------------------------------------------
    #
    def __init__(self):

        a_base.Base.__init__(self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile(r'^\[(.*)\]-\[(.*?)\]$')
        self.epoch = datetime.datetime(1970, 1, 1)


    # --------------------------------------------------------------------------
    #
    def sanity_check(self):
        pass


    def parse_id(self, id):
        # split the id '[rm]-[pid]' in its parts, and return them.

        match = self.id_re.match(id)

        if  not match or len(match.groups()) != 2:
            raise rse.BadParameter("Cannot parse job id '%s'" % id)

        return(match.group(1), match.group(2))


###############################################################################
#
class SLURMJobService(cpi_job.Service):
    ''' Implements cpi_job.Service '''

    # --------------------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        _cpi_base = super(SLURMJobService, self)
        _cpi_base.__init__(api, adaptor)

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
        self._commands = {'sbatch'  : None,
                          'squeue'  : None,
                          'scontrol': None,
                          'scancel' : None}


    # --------------------------------------------------------------------------
    #
    def __del__(self):

        try:
            if self.shell:
                del(self.shell)
        except:
            pass


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, rm_url, session):
        '''
        Service instance constructor
        '''

        self.rm      = rm_url
        self.session = session

        self.jobs = {}
        self._open()

        return self.get_api()


    # --------------------------------------------------------------------------
    #
    def close(self):

        if self.shell:
            self.shell.finalize(True)


    # --------------------------------------------------------------------------
    #
    def _open(self):
        '''
        Open our persistent shell for this job adaptor.  We use
        the pty_shell functionality for this.
        '''

        # check to see what kind of connection we will want to create
        if   self.rm.schema == "slurm":        shell_schema = "fork://"
        elif self.rm.schema == "slurm+ssh":    shell_schema = "ssh://"
        elif self.rm.schema == "slurm+gsissh": shell_schema = "gsissh://"
        else: raise rse.IncorrectURL("Schema %s not supported by SLURM adaptor."
                                     % self.rm.schema)

        # <scheme>://<user>:<pass>@<host>:<port>/<path>?<query>#<fragment>
        # build our shell URL
        shell_url = shell_schema

        # did we provide a username and password?
        if self.rm.username and self.rm.password:
            shell_url += self.rm.username + ":" + self.rm.password + "@"

        # only provided a username
        if self.rm.username and not self.rm.password:
            shell_url += self.rm.username + "@"

        # add hostname
        shell_url += self.rm.host

        # add port
        if  self.rm.port:
            shell_url += ":" + str(self.rm.port)

        shell_url = ru.Url(shell_url)

        # establish shell connection
        self._logger.debug("Opening shell of type: %s" % shell_url)
        self.shell = rsups.PTYShell(shell_url, self.session, self._logger)

        # verify our SLURM environment contains the commands we need for this
        # adaptor to work properly
        self._logger.debug("Verifying existence of remote SLURM tools.")
        for cmd in list(self._commands.keys()):
            ret, out, _ = self.shell.run_sync("which %s " % cmd)
            if ret != 0:
                message = "Error finding SLURM tool %s on remote server %s!\n" \
                          "Locations searched:\n%s\n" \
                          "Is SLURM installed on that machine? " \
                          "If so, is your remote SLURM environment "\
                          "configured properly? " % (cmd, self.rm, out)
                raise rse.NoSuccess._log(self._logger, message)

        self._logger.debug("got cmd prompt (%s)(%s)" % (ret, out))

        self.rm.detected_username = self.rm.username
        # figure out username if it wasn't made explicit
        # important if .ssh/config info read+connected with
        # a different username than what we expect
        if not self.rm.username:
            self._logger.debug("No username provided in URL %s, so we are"
                               " going to find it with whoami" % self.rm)
            ret, out, _ = self.shell.run_sync("whoami")
            self.rm.detected_username = out.strip()
            self._logger.debug("Username detected as: %s",
                               self.rm.detected_username)

        _, out, _ = self.shell.run_sync('scontrol --version')
        self._version = out.split()[1].strip()
        self._logger.info('slurm version: %s' % self._version)

        ppn_pat   = '\'s/.*\\(CPUTot=[0-9]*\\).*/\\1/g\''
        ppn_cmd   = 'scontrol show nodes ' + \
                    '| grep CPUTot'        + \
                    '| sed -e ' + ppn_pat  + \
                    '| sort '              + \
                    '| uniq -c '           + \
                    '| cut -f 2 -d = '     + \
                    '| xargs echo'
        _, out, _ = self.shell.run_sync(ppn_cmd)
        ppn_vals  = [o.strip() for o in out.split() if o.strip()]
        if len(ppn_vals) >= 1: self._ppn = int(ppn_vals[0])
        else                 : self._ppn = None

        if 'stampede2' in self.rm.host.lower():
            # FIXME: this only works on the KNL nodes
            self._ppn = 68

        elif 'traverse' in self.rm.host.lower():
            self._ppn = 32

        elif 'frontera' in self.rm.host.lower():
            # not that this is incorrect for the rtx queue
            self._ppn = 56

        elif 'comet' in self.rm.host.lower():
            self._ppn = 24

        elif 'longhorn' in self.rm.host.lower():
            # FIXME: other option - get it later by `processes_per_host`
            self._ppn = 40

        self._logger.info("ppn: %s", self._ppn)


    # --------------------------------------------------------------------------
    #
    def _close(self):
        '''
        Close our shell connection
        '''

        del(self.shell)
        self.shell = None


    # --------------------------------------------------------------------------
    #
    def _handle_file_transfers(self, ft, mode):
        """
        if mode == 'in' : perform sanity checks on all staging directives.

        if mode == 'in' : stage files to   condor submission site
        if mode == 'out': stage files from condor submission site
        """

        td = TransferDirectives(ft)

        assert(mode in ['in', 'out'])

        if mode == 'in':

            if td.in_append:
                raise Exception('File append (>>) not supported')

            if td.out_append:
                raise Exception('File append (<<) not supported')

            if td.in_overwrite:

                for (local, remote) in td.in_overwrite:

                    source = local
                    target = remote

                    self._logger.info("Transferring in %s to %s", source, target)
                    self.shell.stage_to_remote(source, target)

        elif mode == 'out':

            if td.out_overwrite:

                for (local, remote) in td.out_overwrite:

                    source = remote
                    target = local

                    self._logger.info("Transferring out %s to %s", source, target)
                    self.shell.stage_from_remote(source, target)


    # --------------------------------------------------------------------------
    #
    #
    def _job_run(self, jd):
        '''
        runs a job on the wrapper via pty, and returns the job id
        '''

        # define a bunch of default args
        exe                 = jd.executable
        pre                 = jd.as_dict().get(c.PRE_EXEC)
        post                = jd.as_dict().get(c.POST_EXEC)
        args                = jd.as_dict().get(c.ARGUMENTS, [])
        env                 = jd.as_dict().get(c.ENVIRONMENT, dict())
        cwd                 = jd.as_dict().get(c.WORKING_DIRECTORY)
        job_name            = jd.as_dict().get(c.NAME)
        spmd_variation      = jd.as_dict().get(c.SPMD_VARIATION)
        cpu_count           = jd.as_dict().get(c.TOTAL_CPU_COUNT)
        gpu_count           = jd.as_dict().get(c.TOTAL_GPU_COUNT)
        n_procs             = jd.as_dict().get(c.NUMBER_OF_PROCESSES)
        processes_per_host  = jd.as_dict().get(c.PROCESSES_PER_HOST)
        output              = jd.as_dict().get(c.OUTPUT, "radical.saga.stdout")
        error               = jd.as_dict().get(c.ERROR,  "radical.saga.stderr")
        file_transfer       = jd.as_dict().get(c.FILE_TRANSFER)
        wall_time           = jd.as_dict().get(c.WALL_TIME_LIMIT)
        queue               = jd.as_dict().get(c.QUEUE)
        project             = jd.as_dict().get(c.PROJECT)
        total_memory        = jd.as_dict().get(c.TOTAL_PHYSICAL_MEMORY)
        sys_arch            = jd.as_dict().get(c.SYSTEM_ARCHITECTURE)
        job_contact         = jd.as_dict().get(c.JOB_CONTACT)
        c_hosts             = jd.as_dict().get(c.CANDIDATE_HOSTS)

        cpu_arch            = sys_arch.get('cpu')
        gpu_arch            = sys_arch.get('gpu')

        # check to see what's available in our job description
        # to override defaults

        # try to create the working directory (if defined)
        # NOTE: this assumes a shared filesystem between login node and
        #       compute nodes.
        if cwd:

            self._logger.info("Creating working directory %s" % cwd)
            ret, out, _ = self.shell.run_sync("mkdir -p %s"   % cwd)

            if ret:
                raise rse.NoSuccess("Couldn't create workdir: %s" % out)

        self._handle_file_transfers(file_transfer, mode='in')

        if isinstance(c_hosts, list):
            c_hosts = ','.join(c_hosts)

        if isinstance(job_contact, list):
            job_contact = job_contact[0]

        if project and ':' in project:
            account, reservation = project.split(':', 1)
        else:
            account, reservation = project, None

        script = "#!/bin/sh\n\n"

        # make sure we have something for cpu_count
        if not cpu_count:
            cpu_count = 1

        # make sure we have something for n_procs
        if not n_procs:
            n_procs = cpu_count

        # get memory_per_node from total_memory and make sure it is not None
        memory_per_node = total_memory or 0

        # define n_nodes and recalculate memory_per_node (if self._ppn is set)
        n_nodes = None
        if self._ppn:

            # exception(s) for earlier defined `self._ppn`
            if 'frontera' in self.rm.host.lower() and \
                    queue and 'rtx' in queue.lower():
                self._ppn = 16  # other option is to use: processes_per_host

            n_nodes = int(math.ceil(float(cpu_count) / self._ppn))
            memory_per_node = int(memory_per_node / float(n_nodes))

        elif total_memory:
            raise rse.NotImplemented(
                'cannot allocate memory, node number unknown')

        if spmd_variation:
            if spmd_variation.lower() not in 'mpi':
                raise rse.BadParameter("Slurm cannot handle spmd variation '%s'"
                                        % spmd_variation)
            mpi_cmd = 'mpirun -n %d ' % n_procs

        else:
            # we start N independent processes
            mpi_cmd = ''

            if  'stampede2' in self.rm.host.lower() or \
                'longhorn'  in self.rm.host.lower():

                assert(n_nodes), 'need unique number of cores per node'
                script += "#SBATCH -N %d\n" % n_nodes
                script += "#SBATCH -n %s\n" % n_procs

            elif 'frontera'  in self.rm.host.lower() or \
                 'andes'      in self.rm.host.lower():

                assert(n_nodes), 'need unique number of cores per node'
                script += "#SBATCH -N %d\n" % n_nodes

            elif self._version in ['17.11.5', '18.08.0', '18.08.3']:

                assert(n_nodes), 'need unique number of cores per node'
                script += "#SBATCH -N %d\n" % n_nodes
                script += "#SBATCH --ntasks=%s\n" % n_procs

            else:
                script += "#SBATCH --ntasks=%s\n" % n_procs

            if not processes_per_host:
                script += "#SBATCH --cpus-per-task=%s\n" \
                        % (int(cpu_count / n_procs))

            else:
                script += "#SBATCH --ntasks-per-node=%s\n" % processes_per_host

        # target host specifica
        # FIXME: these should be moved into resource config files
        self._logger.debug ("submit SLURM script to %s", self.rm)
        if 'bridges2' in self.rm.host.lower():
             if gpu_count:
                 # gres resources are specified *per node*
                 assert(n_nodes), 'need unique number of cores per node'
                 script += "#SBATCH --gres=gpu:%s:8\n" % (gpu_arch)

        elif 'comet' in self.rm.host.lower():

            if gpu_count:
                # gres resources are specified *per node*
                assert(n_nodes), 'need unique number of cores per node'

                # if no `gpu_arch` then first available gpu node (either type)
                # gpu types are "p100" and "k80"
                if gpu_arch: gpu_arch_str = ':%s' % gpu_arch.lower()
                else       : gpu_arch_str = ''
                count = 4
                # Make sure we take a full GPU node
                script += "#SBATCH --gres=gpu%s:%d\n" % (gpu_arch_str, count)

        elif 'tiger' in self.rm.host.lower():

            if gpu_count:
                # gres resources are specified *per node*
                assert(n_nodes), 'need unique number of cores per node'
                count = int(gpu_count / n_nodes)

                if count:
                    script += "#SBATCH --gres=gpu:%s\n" % count

        elif 'cori' in self.rm.host.lower():

            # Set to "haswell" for Haswell nodes, to "knl,quad,cache" (or other
            # modes) for KNL, etc.
            if cpu_arch : script += "#SBATCH -C %s\n"     % cpu_arch
            if gpu_count: script += "#SBATCH --gpus=%s\n" % gpu_count

        elif queue == 'tmp3':

            # this is a special queue, which is associated with SuperMUC-NG,
            # but since there is no machine name in config data we only track
            # this queue name to set SLURM QoS option
            script += "#SBATCH --qos=nolimit\n"
            self._logger.debug("SLURM QoS is set (SuperMUC-NG only)\n")

        else:

            if gpu_count: script += "#SBATCH --gpus=%s\n" % gpu_count

        if cwd:
            if 'frontera' in self.rm.host.lower() or \
               'longhorn' in self.rm.host.lower() or \
               'tiger'    in self.rm.host.lower() or \
               'traverse' in self.rm.host.lower() or \
               'andes'     in self.rm.host.lower() or \
               'bridges2' in self.rm.host.lower():
                script += "#SBATCH --chdir %s\n"   % cwd
            else:
                script += "#SBATCH --workdir %s\n" % cwd

        if output         : script += "#SBATCH --output %s\n"      % output
        if error          : script += "#SBATCH --error %s\n"       % error
        if queue          : script += "#SBATCH --partition %s\n"   % queue
        if job_name       : script += '#SBATCH -J "%s"\n'          % job_name
        if c_hosts        : script += "#SBATCH --nodelist=%s\n"    % c_hosts
        if job_contact    : script += "#SBATCH --mail-user=%s\n"   % job_contact
        if account        : script += "#SBATCH --account %s\n"     % account
        if reservation    : script += "#SBATCH --reservation %s\n" % reservation
        if wall_time      : script += "#SBATCH --time %02d:%02d:00\n" \
                                         % (int(wall_time / 60), wall_time % 60)
        if memory_per_node: script += "#SBATCH --mem=%s\n" % memory_per_node

        if env:
            script += "\n## ENVIRONMENT\n"
            for key,val in env.items():
                script += 'export "%s"="%s"\n'  %  (key, val)

        if pre:
            script += "\n## PRE_EXEC\n" + "\n".join(pre)
            script += '\n'

        # create our commandline
        script += "\n## EXEC\n"
        script += '%s%s %s' % (mpi_cmd, exe, ' '.join(args))
        script += '\n'

        if post:
            script += "\n## POST_EXEC\n" + '\n'.join(post)
            script += '\n'

        # write script into a tmp file for staging
        self._logger.info("SLURM script generated:\n%s" % script)

        tgt = os.path.basename(tempfile.mktemp(suffix='.slurm', prefix='tmp_'))
        self.shell.write_to_remote(src=script, tgt=tgt)

        # submit the job
        ret, out, _ = self.shell.run_sync("sbatch '%s'; echo rm -f '%s'" % (tgt,tgt))

        self._logger.debug("submit SLURM script (%s) (%s)" % (tgt, ret))

        # find out what our job ID is
        # TODO: Could make this more efficient
        job_id = None
        for line in out.split("\n"):
            if "Submitted batch job" in line:
                job_id = "[%s]-[%s]" % (self.rm, int(line.split()[-1:][0]))
                break

        # if we have no job ID, there's a failure...
        if not job_id:
            raise rse.NoSuccess._log(self._logger,
                             "Couldn't get job id from submitted job!"
                              " sbatch output:\n%s" % out)

        self._logger.debug("started job %s" % job_id)
        self._logger.debug("Batch system output:\n%s" % out)

        # create local jobs dictionary entry
        self.jobs[job_id] = {'state'      : c.PENDING,
                             'create_time': None,
                             'start_time' : None,
                             'end_time'   : None,
                             'comp_time'  : None,
                             'exec_hosts' : None,
                             'gone'       : False,
                             'output'     : output,
                             'error'      : error,
                             'stdout'     : None,
                             'stderr'     : None,
                             'ft'         : file_transfer,
                             }
        return job_id


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
    def _slurm_to_saga_state(self, slurmjs):
        '''
        translates a slurm one-letter state to saga
        '''

        if   slurmjs in ['CA', "CANCELLED"  ]: return c.CANCELED
        elif slurmjs in ['CD', "COMPLETED"  ]: return c.DONE
        elif slurmjs in ['CF', "CONFIGURING"]: return c.PENDING
        elif slurmjs in ['CG', "COMPLETING" ]: return c.RUNNING
        elif slurmjs in ['F' , "FAILED"     ]: return c.FAILED
        elif slurmjs in ['NF', "NODE_FAIL"  ]: return c.FAILED
        elif slurmjs in ['PD', "PENDING"    ]: return c.PENDING
        elif slurmjs in ['PR', "PREEMPTED"  ]: return c.CANCELED
        elif slurmjs in ['R' , "RUNNING"    ]: return c.RUNNING
        elif slurmjs in ['S' , "SUSPENDED"  ]: return c.SUSPENDED
        elif slurmjs in ['TO', "TIMEOUT"    ]: return c.CANCELED
        else                                 : return c.UNKNOWN


    # --------------------------------------------------------------------------
    #
    def _job_cancel(self, job):
        '''
        Given a job id, attempt to cancel it through use of commandline
        scancel.  Raises exception when unsuccessful.
        '''

        if job._state in c.FINAL:
            # job is already final - nothing to do
            return

        if job._state in [c.NEW]:
            # job is not yet submitted - nothing to do
            job._state = c.CANCELED

        if not job._id:
            # uh oh - what to do?
            raise rse.NoSuccess._log(self._logger,
                    "Could not cancel job: no job ID")

        rm,  pid    = self._adaptor.parse_id(job._id)
        ret, out, _ = self.shell.run_sync("scancel %s" % pid)

        if ret != 0:
            raise rse.NoSuccess._log(self._logger,
                    "Could not cancel job %s because: %s" % (pid, out))

        job._state = c.CANCELED


    # --------------------------------------------------------------------------
    #
    def _job_suspend(self, job):
        '''
        Attempt to suspend a job with commandline scontrol.  Raise
        exception when unsuccessful.
        '''

        if job._state in [c.DONE, c.FAILED, c.CANCELED, c.NEW, c.SUSPENDED]:
            raise rse.IncorrectState._log(self._logger,
                    "Could not suspend job %s [%s]" % (job._id, job._state))


        rm,  pid    = self._adaptor.parse_id(job._id)
        ret, out, _ = self.shell.run_sync("scontrol suspend %s" % pid)

        if ret == 0:
            return True

        # check to see if the error was a permission error
        elif "Access/permission denied" in out:
            raise rse.PermissionDenied._log(self._logger,
                    "Could not suspend job %s because: %s" % (pid, out))

        # it's some other error
        else:
            raise rse.NoSuccess._log(self._logger,
                    "Could not suspend job %s because: %s" % (pid, out))


    # --------------------------------------------------------------------------
    #
    def _job_resume(self, job):
        '''
        Attempt to resume a job with commandline scontrol.  Raise
        exception when unsuccessful.
        '''

        if job._state in [c.DONE, c.FAILED, c.CANCELED, c.NEW, c.RUNNING]:
            raise rse.IncorrectState._log(self._logger,
                    "Could not resume job %s [%s]" % (job._id, job._state))


        rm,  pid    = self._adaptor.parse_id(job._id)
        ret, out, _ = self.shell.run_sync("scontrol resume %s" % pid)

        if ret == 0:
            return True

        # check to see if the error was a permission error
        elif "Access/permission denied" in out:
            raise rse.PermissionDenied._log(self._logger,
                    "Could not suspend job %s because: %s" % (pid, out))

        # it's some other error
        else:
            raise rse.NoSuccess._log(self._logger,
                    "Could not resume job %s because: %s" % (pid, out))


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def create_job(self, jd):
        '''
        Implements cpi_job.Service.create_job()
        '''

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service"    : self,
                         "job_description": jd,
                         "job_schema"     : self.rm.schema,
                         "reconnect"      : False}

        return api_job.Job(_adaptor=self._adaptor, _adaptor_state=adaptor_state)

    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url(self):
        '''
        Implements cpi_job.Service.get_url()
        '''
        return self.rm


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self):
        '''
        Implements rs.adaptors.cpi.job.Service.list()
        '''

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
    def get_job(self, jobid):

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.  The job adaptor will run 'scontrol
        # show job $jobid' to complement the information.
        adaptor_state = {"job_service"    : self,
                         "job_description": api_job.Description(),
                         "job_schema"     : self.rm.schema,
                         "reconnect"      : True,
                         "reconnect_jobid": jobid
                        }
        return api_job.Job(_adaptor=self._adaptor,
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
class SLURMJob(cpi_job.Job):

    # --------------------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        _cpi_base = super(SLURMJob, self)
        _cpi_base.__init__(api, adaptor)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, job_info):

        self.jd = job_info["job_description"]
        self.js = job_info["job_service"]

        # the js is responsible for job bulk operations -- which
        # for jobs only work for run()
        self._container       = self.js
        self._method_type     = "run"

        # initialize job attribute values
        self._id              = None
        self._name            = self.jd.as_dict().get(c.NAME, 'saga')
        self._state           = c.NEW
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

        return self.get_api()


    # --------------------------------------------------------------------------
    #
    def _job_get_info(self):
        '''
        use scontrol to grab job info
        NOT CURRENTLY USED/TESTED, here for later
        '''

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
            curr_info['output'     ] = prev_info.get('output'     )
            curr_info['error'      ] = prev_info.get('error'      )
            curr_info['stdout'     ] = prev_info.get('stdout'     )
            curr_info['stderr'     ] = prev_info.get('stderr'     )
            curr_info['ft'         ] = prev_info.get('ft'         )
        else:
            curr_info['job_id'     ] = None
            curr_info['job_name'   ] = None
            curr_info['state'      ] = None
            curr_info['create_time'] = None
            curr_info['start_time' ] = None
            curr_info['end_time'   ] = None
            curr_info['comp_time'  ] = None
            curr_info['exec_hosts' ] = None
            curr_info['gone'       ] = None
            curr_info['output'     ] = None
            curr_info['error'      ] = None
            curr_info['stdout'     ] = None
            curr_info['stderr'     ] = None
            curr_info['ft'         ] = None

        rm, pid = self._adaptor.parse_id(self._id)

        # update current info with scontrol
        ret, out, _ = self.js.shell.run_sync('scontrol show job %s' % pid)

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
        for elem in sorted(elems):

            parts = elem.split('=', 1)

            if len(parts) == 1:
                # default if no '=' is found
                parts.append(None)

            # ignore non-splittable ones
            key, val = parts
            if val in ['', '(null)']:
                val = None
            self._logger.info('%-20s := %s', key, val)
            data[key] = val

        if data.get('JobState'):
            curr_info['state'] = self.js._slurm_to_saga_state(data['JobState'])
        else:
            curr_info['state'] = self._job_get_state(self._id)

        # update exit code
        if data.get('ExitCode'):
            curr_info['exit_code'] = data['ExitCode'].split(':')[0]
        else:
            curr_info['exit_code'] = self._job_get_state(self._id)

        curr_info['job_name'   ] = data.get('JobName')
      # curr_info['create_time'] = data.get('SubmitTime')
      # curr_info['start_time' ] = data.get('StartTime')
      # curr_info['end_time'   ] = data.get('EndTime')
        curr_info['comp_time'  ] = data.get('RunTime')
        curr_info['exec_hosts' ] = data.get('NodeList')

        # Alas, time stamps are not in EPOCH, and do not contain time zone info,
        # so we set approximate values here
        now = time.time()
        if not curr_info['create_time']: curr_info['create_time'] = now

        if curr_info['state'] in [c.RUNNING] + c.FINAL:
            if not curr_info['start_time' ]: curr_info['start_time' ] = now

        if curr_info['state'] in c.FINAL:

            if not curr_info['end_time' ]: curr_info['end_time' ] = now

            if curr_info['stdout'] is None:

                if curr_info['output'] is None:
                    curr_info['output'] = data.get('StdOut')

                ret, out, err = self.js.shell.run_sync(
                                                 'cat %s' % curr_info['output'])
                if ret: curr_info['stdout'] = None
                else  : curr_info['stdout'] = out

            if curr_info['stderr'] is None:

                if curr_info['error'] is None:
                    curr_info['error'] = data.get('StdErr')

                ret, out, err = self.js.shell.run_sync(
                                                  'cat %s' % curr_info['error'])
                if ret: curr_info['stderr'] = None
                else  : curr_info['stderr'] = out

            self.js._handle_file_transfers(curr_info['ft'], mode='out')

            curr_info['gone'] = True

        self.js.jobs[self._id] = curr_info

        return curr_info


    # --------------------------------------------------------------------------
    #
    def _job_get_state(self, job_id):
        '''
        get the job state from the wrapper shell
        '''

        # if the state is NEW and we haven't sent out a run command, keep
        # it listed as NEW
        if self._state == c.NEW and not self._started:
            return c.NEW

        # if the state is DONE, CANCELED or FAILED, it is considered
        # final and we don't need to query the backend again
        if self._state in c.FINAL:
            return self._state

        rm, pid = self._adaptor.parse_id(job_id)

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
                    return c.UNKNOWN

            return self.js._slurm_to_saga_state(slurm_state)

        except Exception as e:
            self._logger.exception('failed to get job state')
            raise rse.NoSuccess("Error getting the job state for "
                                "job %s:\n%s" % (pid, e)) from e

        raise rse.NoSuccess._log(self._logger, "Internal SLURM adaptor error"
                                 " in _job_get_state")


    # --------------------------------------------------------------------------
    #
    def _sacct_jobstate_match(self, pid):
        '''
        get the job state from the slurm accounting data
        '''

        ret, sacct_out, _ = self.js.shell.run_sync(
            "sacct --format=JobID,State --parsable2 --noheader --jobs=%s" % pid)

        # output will look like:
        # 500723|COMPLETED
        # 500723.batch|COMPLETED
        # or:
        # 500682|CANCELLED by 900369
        # 500682.batch|CANCELLED

        try:
            for line in sacct_out.strip().split('\n'):

                slurm_id, slurm_state = line.split('|', 1)

                if slurm_id == pid and slurm_state:
                    return slurm_state.split()[0].strip()

        except Exception:
            self._logger.warning('cannot parse sacct output:\n%s' % sacct_out)

        return None


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):

        self._state = self._job_get_state(self._id)
        return self._state


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_stdout(self):

        out = self._job_get_info()['stdout']
        if out is None:
            out = ''
          # raise rse.NoSuccess("Couldn't fetch stdout (js reconnected?)")
        return out


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_stderr(self):

        err = self._job_get_info()['stderr']
        if err is None:
            err = ''
          # raise rse.NoSuccess("Couldn't fetch stderr (js reconnected?)")
        return err


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_description(self):

        return self.jd


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_service_url(self):

        return self.js.rm


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def wait(self, timeout):

        time_start = time.time()
        rm, pid    = self._adaptor.parse_id(self._id)

        while True:

            state = self._job_get_state(self._id)
            self._logger.debug("wait() for job id %s:%s" % (self._id, state))

            if state == c.UNKNOWN:
                raise rse.IncorrectState("cannot get job state")

            if state in c.FINAL:
                self._job_get_info()
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
    def get_id(self):

        return self._id


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_name(self):

        if not self._name:
            self._name = self._job_get_info()['job_name']
        return self._name


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code(self):

        # FIXME: use cache
        return self._job_get_info()['exit_code']


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def suspend(self):

        return self.js._job_suspend(self)


    # --------------------------------------------------------------------------
    @SYNC_CALL
    def resume(self):

        return self.js._job_resume(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created(self):

        # FIXME: use cache
        # FIXME: convert to EOPCH
        return self._job_get_info()['create_time']


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started(self):

        # FIXME: use cache
        # FIXME: convert to EPOCH
        return self._job_get_info()['start_time']


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished(self):

        # FIXME: use cache
        # FIXME: convert to EPOCH
        return self._job_get_info()['end_time']


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_execution_hosts(self):

        # FIXME: use cache
        return self._job_get_info()['exec_hosts']


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):

        self.js._job_cancel(self)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):

        self._id      = self.js._job_run(self.jd)
        self._started = True


# ------------------------------------------------------------------------------

