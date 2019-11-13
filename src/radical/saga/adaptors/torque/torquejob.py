
__author__    = "Mark Santcroos, Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2015, The RADICAL Project"
__license__   = "MIT"


""" TORQUE job adaptor implementation
"""

import re
import os
import time
import datetime
import threading

from cgi import parse_qs

import radical.utils as ru

from ...              import exceptions as rse
from ...utils         import pty_shell  as rsups
from ...              import job        as api
from ...adaptors      import base       as a_base
from ...adaptors.cpi  import job        as cpi
from ...adaptors.cpi  import decorators as cpi_decs


SYNC_CALL  = cpi_decs.SYNC_CALL
ASYNC_CALL = cpi_decs.ASYNC_CALL


SYNC_WAIT_UPDATE_INTERVAL =  1  # seconds
MONITOR_UPDATE_INTERVAL   = 60  # seconds


# --------------------------------------------------------------------
#
class _job_state_monitor(threading.Thread):
    """ thread that periodically monitors job states
    """
    def __init__(self, job_service):

        self.logger = job_service._logger
        self.js = job_service
        self._stop = threading.Event()

        super(_job_state_monitor, self).__init__()
        self.setDaemon(True)


    def stop(self):
        self._stop.set()


    def run(self):

        # we stop the monitoring thread when we see the same error 3 times in
        # a row...
        error_type_count = dict()

        while not self._stop.is_set ():

            try:
                # FIXME: do bulk updates here! we don't want to pull information
                # job by job. that would be too inefficient!
                jobs = self.js.jobs

                for job_id in list(jobs.keys()) :

                    job_info = jobs[job_id]

                    # we only need to monitor jobs that are not in a
                    # terminal state, so we can skip the ones that are
                    # either done, failed or canceled
                    if  job_info['state'] not in api.FINAL:

                        # Store the current state since the current state
                        # variable is updated when _job_get_info is called
                        pre_update_state = job_info['state']

                        new_job_info = self.js._job_get_info(job_id,
                                                             reconnect=False)
                        self.logger.info ("Job monitoring thread updating Job "
                                          "%s (old state: %s, new state: %s)" %
                                          (job_id, pre_update_state,
                                           new_job_info['state']))

                        # fire job state callback if 'state' has changed
                        if  new_job_info['state'] != pre_update_state:
                            job_obj = job_info['obj']
                            job_obj._attributes_i_set('state',
                                                      new_job_info['state'],
                                                      job_obj._UP, True)

                        # update job info
                        jobs[job_id] = new_job_info

            except Exception as e:
                import traceback
                traceback.print_exc ()
                self.logger.warning("Exception in job monitor thread: %s" % e)

                # check if we see the same error again and again
                error_type = str(e)
                if  error_type not in error_type_count :
                    error_type_count = dict()
                    error_type_count[error_type]  = 1
                else :
                    error_type_count[error_type] += 1
                    if  error_type_count[error_type] >= 3 :
                        self.logger.error("too many monitoring errors -- stop")
                        return

            finally :
                time.sleep (MONITOR_UPDATE_INTERVAL)


# --------------------------------------------------------------------
#
def log_error_and_raise(message, exception, logger):
    """ logs an 'error' message and subsequently throws an exception
    """
    logger.error(message)
    raise exception(message)


# --------------------------------------------------------------------
#
def _to_saga_jobstate(job_state, retcode, logger=None):
    """ translates one-letter batch system state to saga
    """
    # H: Job is held.
    # Q: Job is queued (eligible to run or routed.)
    # S: Job is suspended.
    # W: Job is waiting for its execution time to be reached.
    # R: Job is running.
    # E: Job is exiting after having run
    # T: Job is being moved to new location.
    # X: Subjob has completed execution or has been deleted (PBSPro)
    # F: Job is Finished (PBSPro)
    # C: Job is completed after having run (Torque)

    ret = None
    if   job_state == 'F':                 # PBSPro
        if retcode ==  0 : ret = api.DONE
        else             : ret = api.FAILED
    elif job_state == 'C':                 # Torque
        if retcode ==  0 : ret = api.DONE
        else             : ret = api.FAILED
    elif job_state == 'H': ret = api.PENDING
    elif job_state == 'Q': ret = api.PENDING
    elif job_state == 'S': ret = api.PENDING
    elif job_state == 'W': ret = api.PENDING
    elif job_state == 'R': ret = api.RUNNING
    elif job_state == 'E': ret = api.RUNNING
    elif job_state == 'T': ret = api.RUNNING
    elif job_state == 'X': ret = api.CANCELED  # PBSPro
    else                 : ret = api.UNKNOWN

    if logger:
        logger.debug('check state: %s', job_state)
        logger.debug('use   state: %s', ret)

    return ret


# --------------------------------------------------------------------
#
def _script_generator(url, logger, jd, ppn, gpn, gres, version,
                      is_cray=False, queue=None):
    '''
    generates a Torque script from a SAGA job description
    '''

    pbs_params  = str()
    exec_n_args = str()

    if jd.executable:
        exec_n_args += "%s " % (jd.executable)

    if jd.arguments:
        for arg in jd.arguments:
            exec_n_args += "%s " % (arg)

    if jd.processes_per_host:
        logger.info("Overriding detected ppn (%d) with processes_per_host (%d)"
                    % (ppn, jd.processes_per_host))
        ppn = jd.processes_per_host

    if jd.name:
        pbs_params += "#PBS -N %s \n" % jd.name

    if is_cray or 'Version: 4.2.7' not in version:
        # qsub on Cray systems complains about the -V option:
        # Warning:
        # Your job uses the -V option, which requests that all of your
        # current shell environment settings (9913 bytes) be exported to
        # it.  This is not recommended, as it causes problems for the
        # batch environment in some cases.
        pbs_params += "#PBS -V \n"

    if jd.environment:
        pbs_params += "#PBS -v %s\n" % \
                ','.join (["%s=%s" % (k,v)
                           for k,v in jd.environment.items()])

    # apparently this doesn't work with older PBS installations
    #
    #   if jd.working_directory:
    #       pbs_params += "#PBS -d %s \n" % jd.working_directory
    #
    # a workaround is to do an explicit 'cd'
    if jd.working_directory:
        workdir_directives  = 'export    PBS_O_WORKDIR=%s \n' \
                                               % jd.working_directory
        workdir_directives += 'mkdir -p  %s\n' % jd.working_directory
        workdir_directives += 'cd        %s\n' % jd.working_directory
    else:
        workdir_directives = ''

    if jd.output:
        # if working directory is set, we want stdout to end up in
        # the working directory as well, unless it containes a specific
        # path name.
        if jd.working_directory:
            if os.path.isabs(jd.output):
                pbs_params += "#PBS -o %s \n" % jd.output
            else:
                # user provided a relative path for STDOUT. in this case
                # we prepend the workind directory path before passing
                # it on to PBS
                pbs_params += "#PBS -o %s/%s \n" % (jd.working_directory,
                                                    jd.output)
        else:
            pbs_params += "#PBS -o %s \n" % jd.output

    if jd.error:
        # if working directory is set, we want stderr to end up in
        # the working directory as well, unless it contains a specific
        # path name.
        if jd.working_directory:
            if os.path.isabs(jd.error):
                pbs_params += "#PBS -e %s \n" % jd.error
            else:
                # user provided a realtive path for STDERR. in this case
                # we prepend the workind directory path before passing
                # it on to PBS
                pbs_params += "#PBS -e %s/%s \n" % (jd.working_directory,
                                                    jd.error)
        else:
            pbs_params += "#PBS -e %s \n" % jd.error


    if jd.wall_time_limit:
        hours = int(jd.wall_time_limit / 60)
        minutes = jd.wall_time_limit % 60
        pbs_params += "#PBS -l walltime=%s:%s:00 \n" \
            % (str(hours), str(minutes))

    # see https://gist.github.com/nobias/5b2373258e595e5242d5
    # The parameter to '-q' can have the following forms:
    #
    #   queue
    #   queue@server
    #   @server
    #
    # where 'server' is the target resource which can be *different* than the
    # submission host.  We interpret 'jd.candidate_hosts[0]' as such a target
    # resource - but only if exactly one `candidate_host` is given.

    # We haqve to take care to filter out special cases where we abise
    # `candidate_hosts` for node properties (those are appended to the nodes
    # argument in the resource_list).  This is currently only implemented for
    # "bigflash" on Gordon@SDSC
    #
    # https://github.com/radical-cybertools/radical.saga/issues/406

    queue_spec      = ''
    node_properties = []

    if      queue: queue_spec =    queue
    elif jd.queue: queue_spec = jd.queue

    # Parse candidate_hosts
    #
    # Currently only implemented for "bigflash" on Gordon@SDSC
    # https://github.com/radical-cybertools/radical.saga/issues/406
    #
    if jd.candidate_hosts:
        if 'BIG_FLASH' in jd.candidate_hosts:
            node_properties.append('bigflash')
        elif len(jd.candidate_hosts) == 1:
            queue_spec += '@%s' % jd.candidate_hosts[0]
        else:
            raise rse.NotImplemented("unsupported candidate_hosts [%s]"
                                % jd.candidate_hosts)
    if queue_spec:
        pbs_params += "#PBS -q %s\n" % queue_spec

    if jd.project:

        if '@' in jd.project:
            user, group = jd.project.split('@', 1)
            pbs_params += "#PBS -A %s \n"            % user
            pbs_params += "#PBS -W group_list:%s \n" % group

        elif 'PBSPro_1' in version:
            # On PBS Pro we set both -P(roject) and -A(accounting),
            # as we don't know what the admins decided, and just
            # pray that this doesn't create problems.
            pbs_params += "#PBS -P %s \n" % str(jd.project)
            pbs_params += "#PBS -A %s \n" % str(jd.project)

        else:
            # Torque
            pbs_params += "#PBS -A %s \n" % str(jd.project)

    if jd.job_contact:
        pbs_params += "#PBS -m abe \n"

    # if total_cpu_count is not defined, we assume 0
    if not jd.total_cpu_count:
        jd.total_cpu_count = 0

    # if total_gpu_count is not defined, we assume 0
    if not jd.total_gpu_count:
        jd.total_gpu_count = 0

    # make sure we want CPUs or GPUs
    assert(jd.total_cpu_count + jd.total_gpu_count > 0)

    # Request enough nodes to cater for the number of cores requested
    cpu_nnodes = 0
    if jd.total_cpu_count:
        cpu_nnodes = jd.total_cpu_count / ppn
        if jd.total_cpu_count % ppn > 0:
            cpu_nnodes += 1

    # Request enough nodes to cater for the number of cores requested
    gpu_nnodes = 0
    if jd.total_gpu_count:
        gpu_nnodes = jd.total_gpu_count / gpn
        if jd.total_gpu_count % gpn > 0:
            gpu_nnodes += 1

    nnodes = max([cpu_nnodes, gpu_nnodes])

    # use the ncpus for systems that need to specify ncpus as multiple of PPN
    ncpus = nnodes * ppn

    if is_cray:
        # Special cases for PBS/TORQUE on Cray. Different PBSes,
        # different flags. A complete nightmare...
        if  'titan'      in url.host       or \
            '5.1.0.h1'   in version or \
            '6.1.1.1.h2' in version or \
            '4.2.6-snap' in version :
            # Can't really use hostname as we run also from the headnode
            logger.info("Using Titan (Cray XP) specific '#PBS -l nodes=xx'")
            pbs_params += "#PBS -l nodes=%d\n" % nnodes
        elif 'edison' in url.host:

            logger.info("Using Edison@NERSC (Cray XC30) (mppwidth=xx)")
            pbs_params += "#PBS -l mppwidth=%s \n" % jd.total_cpu_count
        elif 'bw.ncsa.illinois.edu' in url.host   or \
             'Version: 6.0.4'       in version:
            if gpu_nnodes: gpu_flag = ':xk'
            else         : gpu_flag = ''
            if gpu_flag and ppn > 16: ppn = 16
            logger.info("Using Blue Waters (Cray XE6/XK7) (nodes=xx:ppn=yy)")
            pbs_params += "#PBS -l nodes=%d:ppn=%d%s\n" \
                        % (nnodes, ppn, gpu_flag)
            if 'RS_USE_COMMTRANSPARENT' in os.environ:
                pbs_params += "#PBS -l flags=commtransparent\n"

        elif 'Version: 5.' in version:
            # What would removing this catchall break?
            logger.info("Using TORQUE 5.x notation '#PBS -l procs=XX' ")
            pbs_params += "#PBS -l procs=%d\n" % jd.total_cpu_count
        else:
            logger.info("Using Cray XT (e.g. Kraken, Jaguar) (size=xx)")
            pbs_params += "#PBS -l size=%s\n" % jd.total_cpu_count
    elif 'version: 2.3.13' in version:
        # e.g. Blacklight
        # TODO: The more we add, the more it screams for a refactoring
        pbs_params += "#PBS -l ncpus=%d\n" % ncpus
    elif 'hopper' in url.host:
        logger.info("Using Hopper@NERSC (Cray XE6) (mppwidth=xx)")
        pbs_params += "#PBS -l mppwidth=%s \n" % jd.total_cpu_count
    elif 'rhea.ccs.ornl.gov' in url.host or 'rhea-login' in os.uname()[1]:
        # Not allowed to specify ppn on Rhea
        pbs_params += "#PBS -l nodes=%d\n" % (nnodes)
    else:
        # Default case, i.e, standard HPC cluster (non-Cray)

        # If we want just a slice of one node
        if jd.total_cpu_count < ppn:
            ppn = jd.total_cpu_count

      # pbs_params += "#PBS -l nodes=%d:ppn=%d%s\n" % (
        pbs_params += "#PBS -l nodes=%d%s\n" % (
            nnodes, ''.join([':%s' % prop for prop in node_properties]))

    # Process Generic Resource specification request
    if gres:
        pbs_params += "#PBS -l gres=%s\n" % gres

    exec_n_args += 'export SAGA_PPN=%d\n' % ppn
    exec_n_args += 'export SAGA_GPN=%d\n' % gpn

    # escape all double quotes and dollarsigns, otherwise 'echo |'
    # further down won't work
    # only escape '$' in args and exe. not in the params
    exec_n_args = workdir_directives + exec_n_args
    exec_n_args = exec_n_args.replace('$', '\\$')

    pbscript = "\n#!/bin/bash \n%s%s" % (pbs_params, exec_n_args)

    pbscript = pbscript.replace('"', '\\"')
    return pbscript


# --------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "radical.saga.adaptors.torquejob"
_ADAPTOR_SCHEMAS       = ["torque", "torque+ssh", "torque+gsissh"]
_ADAPTOR_OPTIONS       = []

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES = {
    "jdes_attributes":   [api.NAME,
                          api.EXECUTABLE,
                          api.ARGUMENTS,
                          api.CANDIDATE_HOSTS,
                          api.ENVIRONMENT,
                          api.INPUT,
                          api.OUTPUT,
                          api.ERROR,
                          api.QUEUE,
                          api.PROJECT,
                          api.FILE_TRANSFER,
                          api.WALL_TIME_LIMIT,
                          api.WORKING_DIRECTORY,
                          api.WALL_TIME_LIMIT,
                          api.PROCESSES_PER_HOST,
                          api.SPMD_VARIATION,
                          api.TOTAL_CPU_COUNT,
                          api.TOTAL_GPU_COUNT,
                         ],
    "job_attributes":    [api.EXIT_CODE,
                          api.EXECUTION_HOSTS,
                          api.CREATED,
                          api.STARTED,
                          api.FINISHED],
    "metrics":           [api.STATE],
    "callbacks":         [api.STATE],
    "contexts":          {"ssh": "SSH public/private keypair",
                          "x509": "GSISSH X509 proxy context",
                          "userpass": "username/password pair (ssh)"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC = {
    "name":          _ADAPTOR_NAME,
    "cfg_options":   _ADAPTOR_OPTIONS,
    "capabilities":  _ADAPTOR_CAPABILITIES,
    "description":  """
The TORQUE adaptor allows to run and manage jobs on
`TORQUE <http://www.adaptivecomputing.com/products/open-source/torque>`_
controlled HPC clusters.
""",
    "example": "examples/jobs/torquejob.py",
    "schemas": {"torque":        "connect to a local cluster",
                "torque+ssh":    "connect to a remote cluster via SSH",
                "torque+gsissh": "connect to a remote cluster via GSISSH"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA
#
_ADAPTOR_INFO = {
    "name"        : _ADAPTOR_NAME,
    "version"     : "v0.1",
    "schemas"     : _ADAPTOR_SCHEMAS,
    "capabilities": _ADAPTOR_CAPABILITIES,
    "cpis": [
                {
                    "type": "radical.saga.job.Service",
                    "class": "TORQUEJobService"
                },
                {
                    "type": "radical.saga.job.Job",
                    "class": "TORQUEJob"
                }
            ]
}


###############################################################################
# The adaptor class
class Adaptor (a_base.Base):
    """ this is the actual adaptor class, which gets loaded by SAGA (i.e. by
        the SAGA engine), and which registers the CPI implementation classes
        which provide the adaptor's functionality.
    """

    # ----------------------------------------------------------------
    #
    def __init__(self):

        a_base.Base.__init__(self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile('^\[(.*)\]-\[(.*?)\]$')
        self.epoch = datetime.datetime(1970, 1, 1)


    # ----------------------------------------------------------------
    #
    def sanity_check(self):
        # FIXME: also check for gsissh
        pass


    # ----------------------------------------------------------------
    #
    def parse_id(self, id):
        # split the id '[rm]-[pid]' in its parts, and return them.

        match = self.id_re.match(id)

        if not match or len(match.groups()) != 2:
            raise rse.BadParameter("Cannot parse job id '%s'" % id)

        return (match.group(1), match.group(2))


###############################################################################
#
class TORQUEJobService (cpi.Service):
    """ implements cpi.Service
    """

    # ----------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        self._mt  = None
        _cpi_base = super(TORQUEJobService, self)
        _cpi_base.__init__(api, adaptor)

        self._adaptor = adaptor

    # ----------------------------------------------------------------
    #
    def __del__(self):

        self.close()


    # ----------------------------------------------------------------
    #
    def close(self):

        if  self.mt :
            self.mt.stop()
            self.mt.join(10)  # don't block forever on join()

        self._logger.info("Job monitoring thread stopped.")
        self.finalize(True)


    # ----------------------------------------------------------------
    #
    def finalize(self, kill_shell=False):

        if  kill_shell :
            if  self.shell :
                self.shell.finalize (True)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, rm_url, session):
        """ service instance constructor
        """
        self.rm      = rm_url
        self.session = session
        self.ppn     = None
        self.gpn     = 1   # gpus per node  # FIXME: inspect system
        self.is_cray = False
        self.queue   = None
        self.shell   = None
        self.jobs    = dict()
        self.gres    = None

        # the monitoring thread - one per service instance
        self.mt = _job_state_monitor(job_service=self)
        self.mt.start()

        rm_scheme = rm_url.scheme
        pty_url   = ru.Url(rm_url)

        # this adaptor supports options that can be passed via the
        # 'query' component of the job service URL.
        if rm_url.query:
            for key, val in parse_qs(rm_url.query).items():
                if key == 'queue':
                    self.queue = val[0]
                elif key == 'craytype':
                    self.is_cray = val[0]
                elif key == 'ppn':
                    self.ppn = int(val[0])
                elif key == 'gres':
                    self.gres = val[0]


        # we need to extract the scheme for PTYShell. That's basically the
        # job.Service Url without the pbs+ part. We use the PTYShell to execute
        # pbs commands either locally or via gsissh or ssh.
        scheme_elems = rm_scheme.split('+')
        if   'gsissh' in scheme_elems : pty_url.scheme = "gsissh"
        elif 'ssh'    in scheme_elems : pty_url.scheme = "ssh"
        else                          : pty_url.scheme = "fork"

        # these are the commands that we need in order to interact with PBS.
        # the adaptor will try to find them during initialize(self) and bail
        # out in case they are note available.
        self._commands = {'pbsnodes': None,
                          'qstat':    None,
                          'qsub':     None,
                          'qdel':     None,
                          'checkjob': None}

        self.shell = rsups.PTYShell(pty_url, self.session)

      # self.shell.set_initialize_hook(self.initialize)
      # self.shell.set_finalize_hook(self.finalize)

        self.initialize()
        return self.get_api()


    # ----------------------------------------------------------------
    #
    def initialize(self):

        # check if all required pbs tools are available
        ret, out, _ = self.shell.run_sync("qstat --version")

        if ret:
            message = "Error finding PBS tools: %s" % out
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        self._pbs_version = out.strip()
        self._logger.info("Found PBS version: %s" % self._pbs_version)

        for cmd in self._commands:

            ret, out, _ = self.shell.run_sync("which %s " % cmd)

            if ret:
                self._logger.warn("missing PBS tool %s: %s" % (cmd, out))
                continue

            self._commands[cmd] = out.strip()  # strip removes newline
            self._logger.info("Found PBS %s: %s" % (cmd, out.strip()))


        # TODO: Get rid of this, as I dont think there is any justification that
        #       Cray's are special
        #
        # let's try to figure out if we're working on a Cray machine.
        # naively, we assume that if we can find the 'aprun' command in the
        # path that we're logged in to a Cray machine.
        if not self.is_cray:
            ret, out, _ = self.shell.run_sync('which aprun')
            if ret != 0:
                self.is_cray = False
            else:
                self._logger.info("Host '%s' seems to be a Cray machine."
                    % self.rm.host)
                self.is_cray = True
        else:
            self._logger.info("host is cray: %s" % self.is_cray)

        #
        # Get number of processes per node
        #
        if self.ppn:
            self._logger.debug("Using user specified 'ppn': %d" % self.ppn)
            return

        # TODO: this is quite a hack. however, it *seems* to work quite
        #       well in practice.
        if any(ver in  self._pbs_version
                   for ver in ('PBSPro_13', 'PBSPro_12', 'PBSPro_11.3')):
            ret, out, _ = self.shell.run_sync('unset GREP_OPTIONS; %s -a '
                          '| grep -E "resources_available.ncpus"'
                          % self._commands['pbsnodes'])
        else:
            ret, out, _ = self.shell.run_sync('unset GREP_OPTIONS; %s -a '
                          '| grep -E "(np|pcpu|pcpus)[[:blank:]]*=" '
                          % self._commands['pbsnodes'])

        if ret != 0:
            message = "Error running pbsnodes: %s" % out
            log_error_and_raise(message, rse.NoSuccess, self._logger)
        else:
            # this is black magic. we just assume that the highest occurrence
            # of a specific np is the number of processors (cores) per compute
            # node. this equals max "PPN" for job scripts
            ppn_list = dict()
            for line in out.split('\n'):
                np = line.split(' = ')
                if len(np) == 2:
                    np_str = np[1].strip()
                    if np_str == '<various>':
                        continue
                    else:
                        np = int(np_str)
                    if np in ppn_list:
                        ppn_list[np] += 1
                    else:
                        ppn_list[np] = 1
            self.ppn = max(ppn_list, key=ppn_list.get)
            self._logger.debug("Found the following 'ppn' configurations: %s. "
                "Using %s as default ppn."  % (ppn_list, self.ppn))

    # ----------------------------------------------------------------
    #
    def _job_run(self, job_obj):
        """ runs a job via qsub
        """

        # get the job description
        jd       = job_obj.get_description()
        job_name = jd.name

        # normalize working directory path
        if  jd.working_directory :
            jd.working_directory = os.path.normpath (jd.working_directory)

        # TODO: Why would one want this?
        if self.queue and jd.queue:
            self._logger.warning("Job service was instantiated explicitly with \
'queue=%s', but job description tries to a different queue: '%s'. Using '%s'." %
                                (self.queue, jd.queue, self.queue))

        try:
            # create a PBS job script from SAGA job description
            script = _script_generator(url=self.rm, logger=self._logger,
                                   jd=jd, ppn=self.ppn, gpn=self.gpn,
                                   gres=self.gres,
                                   version=self._pbs_version,
                                   is_cray=self.is_cray, queue=self.queue
                                   )

            self._logger.info("Generated PBS script: %s" % script)
        except Exception as ex:
            log_error_and_raise(str(ex), rse.BadParameter, self._logger)

        # try to create the working directory (if defined)
        # WARNING: this assumes a shared filesystem between login node and
        #          compute nodes.
        if jd.working_directory:
            self._logger.info("Create workdir %s" % jd.working_directory)
            ret, out, _ = self.shell.run_sync("mkdir -p %s"
                                             % jd.working_directory)
            if ret != 0:
                # something went wrong
                message = "Couldn't create working directory - %s" % (out)
                log_error_and_raise(message, rse.NoSuccess, self._logger)

        # Now we want to execute the script. This process consists of two steps:
        # (1) we create a temporary file with 'mktemp' and write the contents of
        #     the generated PBS script into it
        # (2) we call 'qsub <tmpfile>' to submit the script to the
        #     queueing system
        cmdline = """
        SCRIPTFILE=`mktemp -t rs.jobscript.XXXXXX` \\
            &&  echo "%s" > $SCRIPTFILE \\
            &&  %s $SCRIPTFILE \\
            &&  rm -f $SCRIPTFILE
            """ %  (script, self._commands['qsub'])
        ret, out, _ = self.shell.run_sync(cmdline)

        if ret != 0:
            # something went wrong
            message = "Error running job via 'qsub': %s. Commandline was: %s" \
                % (out, cmdline)
            log_error_and_raise(message, rse.NoSuccess, self._logger)
        else:
            # parse the job id. qsub usually returns just the job id, but
            # sometimes there are a couple of lines of warnings before.
            # if that's the case, we log those as 'warnings'
            lines = out.split('\n')
            lines = [lines for lines in lines if lines != '']  # remove empty

            if len(lines) > 1:
                self._logger.warning('qsub: %s' % ''.join(lines[:-2]))

            # we asssume job id is in the last line
            job_id = "[%s]-[%s]" % (self.rm, lines[-1].strip().split('.')[0])
            self._logger.info("Submitted PBS job with id: %s" % job_id)

            state = api.PENDING

            # populate job info dict
            self.jobs[job_id] = {'obj'         : job_obj,
                                 'job_id'      : job_id,
                                 'name'        : job_name,
                                 'state'       : state,
                                 'exec_hosts'  : None,
                                 'returncode'  : None,
                                 'create_time' : time.time(),
                                 'start_time'  : None,
                                 'end_time'    : None,
                                 'gone'        : False
                                 }

            self._logger.info ("assign job id  %s / %s / %s to watch list (%s)"
                            % (job_name, job_id, job_obj, list(self.jobs.keys())))

            # set status to 'pending' and manually trigger callback
            job_obj._attributes_i_set('state', state, job_obj._UP, True)

            # return the job id
            return job_id


    # ----------------------------------------------------------------
    #
    def _job_get_info(self, job_id, reconnect):
        """ Get job information attributes via qstat.
        """

        # If we don't have the job in our dictionary, we don't want it,
        # unless we are trying to reconnect.
        if not reconnect and job_id not in self.jobs:
            message = "Unknown job id: %s. Can't update state." % job_id
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        if not reconnect:
            # job_info contains the info collect when _job_get_info
            # was called the last time
            job_info = self.jobs[job_id]

            # if the 'gone' flag is set, there's no need to query the job
            # state again. it's gone forever
            if job_info['gone'] is True:
                return job_info
        else:
            # Create a template data structure
            job_info = {
                'job_id':       job_id,
                'state':        api.UNKNOWN,
                'name':         None,
                'exec_hosts':   None,
                'returncode':   None,
                'create_time':  None,
                'start_time':   None,
                'end_time':     None,
                'gone':         False
            }

        rm, pid = self._adaptor.parse_id(job_id)

        # try `qstat`.  If that doesn't work, fall back to `checkjob`
        ok = False

        # TODO: move to config file
        #       if 'PBSPro_1' in self._pbs_version:
        qstat_flag = '-f1'

        ret, out1, err1 = self.shell.run_sync("unset GREP_OPTIONS; %s %s %s | "
                "grep -E -i '(job_state)|(exec_host)|(exit_status)|"
                 "(ctime)|(start_time)|(stime)|(mtime)'"
                % (self._commands['qstat'], qstat_flag, pid))

        if ret != 0:
            self._logger.warn('qstat failed with: %s', err1)

        else:

            # qstat worked - parse output
            ok = True

            # the result should look something like this:
            #     job_state = C
            #     exec_host = i72/0
            #     exit_status = 0
            for line in out1.split('\n'):

                if '=' not in line:
                    continue

                key, val = line.split('=', 1)
                key = key.strip().lower
                val = val.strip()

                if   key in ['job_state'  ]: job_state = val
                elif key in ['job_name'   ]: job_info['name'] = val
                elif key in ['exit_status']: job_info['returncode' ] = int(val)
                elif key in ['exec_host'  ]: job_info['exec_hosts' ] = val.split('+')
                                             # format i73/7+i73/6+...

              # FIXME: qstat will not tell us time zones, so we cannot
              #        convert to EPOCH (which is UTC).  We thus take
              #        times ourself.  A proper solution would be to
              #        either do the time conversion on the target host,
              #        or to inspect time zone settings on the host.
              #
              # # PBS Pro doesn't provide "end time", but
              # # "resources_used.walltime" could be added up to the
              # # start time.  Alternatively, we can use mtime, (latest
              # # modification time) which is generally also end time.
              # # TORQUE has an "comp_time" (completion? time), that is
              # # generally the same as mtime.  # # For now we  use
              # mtime for both TORQUE and PBS Pro.

                elif key in ['start_time',  # TORQUE / PBS Pro
                             'stime'      ]: job_info['start_time' ] = val
                elif key in ['ctime'      ]: job_info['create_time'] = val
                elif key in ['mtime'      ]: job_info['end_time'   ] = val


        if not ok:

            # qstat failed - try `checkjob`
            ret, out2, err2 = self.shell.run_sync("%s %s"
                    % (self._commands['checkjob'], pid))

            if ret != 0:
                self._logger.warn('checkjob failed with: %s', err2)

            else:

                # checkjob worked - parse result
                ok = True

                # the result should look something like this:
                #     job 4683422
                #
                #     AName: titan.qsub
                #     State: Running
                #     Creds:  user:merzky1  group:merzky1  account:BIP149 \
                #             class:debug  qos:smallplcmnt
                #     WallTime:   00:00:52 of 00:05:00
                #     SubmitTime: Mon Apr 29 06:38:53
                #     ...
                #
                # See also parse comments on qstat values
                # FIXME: exec host parsing
                # FIXME: missing start time (should be computed from walltime)
                for line in out2.split('\n'):

                    if not ':' in line:
                        continue

                    key, val = line.split(':', 1)
                    key = key.strip().lower()
                    val = val.strip()

                    if   key in ['state'     ]: job_state               = val
                    elif key in ['aname'     ]: job_info['name']        = val
                    elif key in ['submittime']: job_info['create_time'] = val

                    elif key in ['completion code']:
                        job_info['end_time'  ] = str(val.split(':', 1)[1])
                        job_info['returncode'] = int(val.split(      )[0])

        if ok:

            # we did get some information - see if we need a state update
            # TORQUE doesn't allow us to distinguish DONE/FAILED on
            # final state alone,  we need to consider the exit_status.
            retcode = job_info.get('returncode', -1)
            job_info['state'] = _to_saga_jobstate(job_state, retcode,
                                                  logger=self._logger)

            # FIXME: workaround for time zone problem described above
            if job_info['state'] in [api.RUNNING] + api.FINAL \
                and not job_info['start_time']:
                job_info['start_time'] = time.time()

            if job_info['state'] in api.FINAL \
                and not job_info['end_time']:
                job_info['end_time'] = time.time()


        else:

            # both failed - search all output for 'unknown job' or 'invalid job'
            all_out = '\n'.join([out1, out2, err1, err2])

            if  "unknown job" in all_out.lower() or \
                "invalid job" in all_out.lower():

                # Let's see if the last known job state was running or pending.
                # in that case, the job is gone now, which can either mean DONE,
                # or FAILED. the only thing we can do is set it to 'DONE'
                job_info['gone'] = True

                # TODO: we can also set the end time?
                self._logger.warning("Previously running job has disappeared. "
                    "This probably means that the backend doesn't store "
                    "information about finished jobs. Setting state to 'DONE'.")

                if job_info['state'] in [api.RUNNING, api.PENDING]:
                    job_info['state']      = api.DONE
                    job_info['returncode'] = 0  # we are guessing here...
                else:
                    job_info['state'] = api.UNKNOWN

                if not job_info['start_time']:
                    # inaccurate guess, but better than nothing
                    job_info['start_time'] = time.time()

                if not job_info['end_time']:
                    # inaccurate guess, but better than nothing
                    job_info['end_time'] = time.time()

            else:
                if reconnect:
                    log_error_and_raise("Couldn't reconnect to '%s'" % job_id,
                                        rse.NoSuccess, self._logger)

                else:
                    log_error_and_raise("Error retrieving info for %s" % job_id,
                                        rse.NoSuccess, self._logger)

        # return the updated job info
        return job_info


    # ----------------------------------------------------------------
    #
    def _parse_qstat(self, haystack, job_info):


        # return the new job info dict
        return job_info

    # ----------------------------------------------------------------
    #
    def _job_get_state(self, job_id):
        """ get the job's state
        """
        return self.jobs[job_id]['state']

    # ----------------------------------------------------------------
    #
    def _job_get_exit_code(self, job_id):
        """ get the job's exit code
        """
        ret = self.jobs[job_id]['returncode']

        if ret is None : return None
        else           : return int(ret)

    # ----------------------------------------------------------------
    #
    def _job_get_execution_hosts(self, job_id):
        """ get the job's exit code
        """
        return self.jobs[job_id]['exec_hosts']

    # ----------------------------------------------------------------
    #
    def _job_get_create_time(self, job_id):
        """ get the job's creation time
        """
        return self.jobs[job_id]['create_time']

    # ----------------------------------------------------------------
    #
    def _job_get_start_time(self, job_id):
        """ get the job's start time
        """
        return self.jobs[job_id]['start_time']

    # ----------------------------------------------------------------
    #
    def _job_get_end_time(self, job_id):
        """ get the job's end time
        """
        return self.jobs[job_id]['end_time']

    # ----------------------------------------------------------------
    #
    def _job_cancel(self, job_id):
        """ cancel the job via 'qdel'
        """
        rm, pid = self._adaptor.parse_id(job_id)

        ret, out, _ = self.shell.run_sync("%s %s\n"
                                       % (self._commands['qdel'], pid))

        if ret != 0:
            message = "Error canceling job via 'qdel': %s" % out
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        # assume the job was succesfully canceled
        self.jobs[job_id]['state'] = api.CANCELED

        if not self.jobs[job_id]['end_time']:
            self.jobs[job_id]['end_time'] = time.time()


    # ----------------------------------------------------------------
    #
    def _job_wait(self, job_id, timeout):
        """ wait for the job to finish or fail
        """
        time_start = time.time()
        time_now   = time_start
        rm, pid    = self._adaptor.parse_id(job_id)

        while True:
            state = self.jobs[job_id]['state']  # this gets updated in the bg.

            if state in api.FINAL:
                return True

            # avoid busy poll
            time.sleep(SYNC_WAIT_UPDATE_INTERVAL)

            # check if we hit timeout
            if timeout >= 0:
                time_now = time.time()
                if time_now - time_start > timeout:
                    return False

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def create_job(self, jd):
        """ implements cpi.Service.get_url()
        """
        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         "job_description": jd,
                         "job_schema":      self.rm.schema,
                         "reconnect":       False
                         }

        # create and return a new job object
        return api.Job(_adaptor=self._adaptor,
                       _adaptor_state=adaptor_state)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job(self, job_id):
        """ Implements cpi.Service.get_job()

            Re-create job instance from a job-id.
        """

        # If we already have the job info, we just pass the current info.
        if job_id in self.jobs :
            return self.jobs[job_id]['obj']

        # Try to get some initial information about this job (again)
        job_info = self._job_get_info(job_id, reconnect=True)

        # create job description from queue info
        # FIXME: incomplete
        jd      = api.Description()
        jd.name = job_info['name']

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         "job_description": jd,
                         "job_schema":      self.rm.schema,
                         "reconnect":       True,
                         "reconnect_jobid": job_id
                         }

        job_obj = api.Job(_adaptor=self._adaptor,
                          _adaptor_state=adaptor_state)

        # throw it into our job dictionary.
        job_info['obj']   = job_obj
        self.jobs[job_id] = job_info

        return job_obj

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url(self):
        """ implements cpi.Service.get_url()
        """
        return self.rm

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self):
        """ implements cpi.Service.list()
        """
        ids = []

        ret, out, _ = self.shell.run_sync(
                                     "unset GREP_OPTIONS; %s | grep `whoami`"
                                     % self._commands['qstat'])

        if ret != 0 and len(out) > 0:
            message = "failed to list jobs via 'qstat': %s" % out
            log_error_and_raise(message, rse.NoSuccess, self._logger)
        elif ret != 0 and len(out) == 0:
            # qstat | grep `` exits with 1 if the list is empty
            pass
        else:
            for line in out.split("\n"):
                # output looks like this:
                # 112059.svc.uc.futuregrid testjob oweidner 0 Q batch
                # 112061.svc.uc.futuregrid testjob oweidner 0 Q batch
                if len(line.split()) > 1:
                    job_id = "[%s]-[%s]" % (self.rm,
                                            line.split()[0].split('.')[0])
                    ids.append(str(job_id))

        return ids


    # ----------------------------------------------------------------
    #
    def container_run (self, jobs) :

        self._logger.debug ("container run: %s"  %  str(jobs))

        # TODO: this is not optimized yet
        for job in jobs:
            job.run ()


    # ----------------------------------------------------------------
    #
    def container_wait (self, jobs, mode, timeout) :

        self._logger.debug ("container wait: %s"  %  str(jobs))

        # TODO: this is not optimized yet
        for job in jobs:
            job.wait ()


    # ----------------------------------------------------------------
    #
    def container_cancel (self, jobs, timeout) :

        self._logger.debug ("container cancel: %s"  %  str(jobs))

        # TODO: this is not optimized yet
        for job in jobs:
            job.cancel (timeout)


################################################################################
#
class TORQUEJob (cpi.Job):
    """ implements cpi.Job
    """


    def __init__(self, api, adaptor):

        # initialize parent class
        _cpi_base = super(TORQUEJob, self)
        _cpi_base.__init__(api, adaptor)


    def _get_impl(self):
        return self


    @SYNC_CALL
    def init_instance(self, job_info):
        """ implements cpi.Job.init_instance()
        """

        # init_instance is called for every new api.Job object
        # that is created
        self.jd = job_info["job_description"]
        self.js = job_info["job_service"]

        if job_info['reconnect'] is True:
            self._id      = job_info['reconnect_jobid']
            self._name    = self.jd.name
            self._started = True
        else:
            self._id      = None
            self._name    = self.jd.name
            self._started = False

        return self.get_api()


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        """ implements cpi.Job.get_state()
        """

        if  self._started is False:
            return api.NEW

        return self.js._job_get_state(job_id=self._id)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def wait(self, timeout):
        """ implements cpi.Job.wait()
        """

        if self._started is False:
            log_error_and_raise("Can't wait for job that hasn't been started",
                rse.IncorrectState, self._logger)
        else:
            self.js._job_wait(job_id=self._id, timeout=timeout)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):
        """ implements cpi.Job.cancel()
        """

        if self._started is False:
            log_error_and_raise("Can't wait for job that hasn't been started",
                rse.IncorrectState, self._logger)
        else:
            self.js._job_cancel(self._id)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):
        """ implements cpi.Job.run()
        """

        self._id = self.js._job_run(self._api())
        self._started = True


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_service_url(self):
        """ implements cpi.Job.get_service_url()
        """

        return self.js.rm


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id(self):
        """ implements cpi.Job.get_id()
        """

        return self._id


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_name (self):
        """ Implements cpi.Job.get_name() """

        return self._name


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code(self):
        """ implements cpi.Job.get_exit_code()
        """
        if self._started: return self.js._job_get_exit_code(self._id)
        else            : return None


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created(self):
        """ implements cpi.Job.get_created()
        """
        if self._started: return self.js._job_get_create_time(self._id)
        else            : return None


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started(self):
        """ implements cpi.Job.get_started()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_start_time(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished(self):
        """ implements cpi.Job.get_finished()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_end_time(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_execution_hosts(self):
        """ implements cpi.Job.get_execution_hosts()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_execution_hosts(self._id)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_description(self):
        """ implements cpi.Job.get_execution_hosts()
        """
        return self.jd


# ------------------------------------------------------------------------------

