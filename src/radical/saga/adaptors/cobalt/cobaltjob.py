
__author__    = "Andre Merzky, Ole Weidner, Mark Santcroos, Manuel Maldonado"
__copyright__ = "Copyright 2012-2016, The SAGA Project"
__license__   = "MIT"


""" Cobalt job adaptor implementation
"""

import re
import os 
import time
import datetime
import threading

from cgi  import parse_qs

from ...url           import Url
from ...              import exceptions as rse
from ...utils         import pty_shell  as rsups
from ...              import job        as api
from ..               import base       as a_base
from ..cpi            import job        as cpi_job
from ..cpi            import decorators as cpi_decs


SYNC_CALL  = cpi_decs.SYNC_CALL
ASYNC_CALL = cpi_decs.ASYNC_CALL

SYNC_WAIT_UPDATE_INTERVAL =  1  # seconds
MONITOR_UPDATE_INTERVAL   = 60  # seconds


# ------------------------------------------------------------------------------
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
                    if  job_info['state'] \
                        not in [api.DONE, api.FAILED, api.CANCELED]:

                        # Store the current state since the current state 
                        # variable is updated when _job_get_info is called
                        pre_update_state = job_info['state']

                        new_job_info = self.js._job_get_info(job_id, reconnect=False)
                        self.logger.info ("Job monitoring thread updating Job "
                                          "%s (old state: %s, new state: %s)" % 
                                          (job_id, pre_update_state, new_job_info['state']))

                        # fire job state callback if 'state' has changed
                        if  new_job_info['state'] != pre_update_state:
                            job_obj = job_info['obj']
                            job_obj._attributes_i_set('state', new_job_info['state'], job_obj._UP, True)

                        # update job info
                        jobs[job_id] = new_job_info

            except Exception as e:
                import traceback
                traceback.print_exc()
                self.logger.warning("Exception caught in job monitoring thread: %s" % e)

                # check if we see the same error again and again
                error_type = str(e)
                if  error_type not in error_type_count :
                    error_type_count = dict()
                    error_type_count[error_type]  = 1
                else :
                    error_type_count[error_type] += 1
                    if  error_type_count[error_type] >= 3 :
                        self.logger.error("too many monitoring errors -- stopping job monitoring thread")
                        return

            finally :
                time.sleep (MONITOR_UPDATE_INTERVAL)


# ------------------------------------------------------------------------------
#
def log_error_and_raise(message, exception, logger):
    """ logs an 'error' message and subsequently throws an exception
    """
    logger.error(message)
    raise exception(message)


# ------------------------------------------------------------------------------
#
def _cobalt_to_saga_jobstate(cobaltjs):
    """ translates a cobalt one-letter state to saga
    """

    if   cobaltjs == 'C': return api.DONE
    elif cobaltjs == 'F': return api.DONE
    elif cobaltjs == 'H': return api.PENDING   # held by user or dependency
    elif cobaltjs == 'Q': return api.PENDING   # queued
    elif cobaltjs == 'S': return api.PENDING   # suspended
    elif cobaltjs == 'W': return api.PENDING   # waiting for execution
    elif cobaltjs == 'R': return api.RUNNING   # starting/running
    elif cobaltjs == 'E': return api.RUNNING   # exiting after having run
    elif cobaltjs == 'T': return api.RUNNING   # being moved to new location
    elif cobaltjs == 'X': return api.CANCELED  # Subjob completed or deleted
    else                : return api.UNKNOWN


# ------------------------------------------------------------------------------
#
def _cobaltscript_generator(url, logger, jd, ppn, is_cray=False, queue=None,
                            run_job='/usr/bin/runjob'):
    """
    Generates Cobalt-style 'qsub' command arguments from a SAGA job description
    """
    cobalt_params       = str()
    exec_n_args         = str()
    cobaltscript        = str()
    total_cpu_count     = None
    number_of_processes = None
    processes_per_host  = None
    blue_gene_q_modes   = [1, 2, 4, 8, 16, 32, 64]

    ppn = 16  # for now, defaulting to number of cores per node in Blue Gene/Q

    if jd.executable:
        exec_n_args += "%s " % (jd.executable)
    if jd.arguments:
        for arg in jd.arguments:
            exec_n_args += "%s " % (arg)

    if jd.name:
        cobalt_params += '#COBALT --jobname %s\n' % jd.name

    if jd.working_directory:
        cobalt_params += '#COBALT --cwd %s\n' % jd.working_directory

    # a workaround is to do an explicit 'cd'
    # if jd.working_directory:
    #     workdir_directives  = 'export    PBS_O_WORKDIR=%s \n' \
    #                                            % jd.working_directory
    #     workdir_directives += 'mkdir -p  %s\n' % jd.working_directory
    #     workdir_directives += 'cd        %s\n' % jd.working_directory
    # else:
    #     workdir_directives = ''

    if jd.output:
        # if working directory is set, we want stdout to end up in
        # the working directory as well, unless it containes a specific
        # path name.
        if jd.working_directory:
            if os.path.isabs(jd.output):
                cobalt_params += '#COBALT --output %s\n' % jd.output
            else:
                # user provided a relative path for STDOUT. in this case 
                # we prepend the workind directory path before passing
                # it on to Cobalt
                cobalt_params += '#COBALT --output %s/%s\n' \
                               % (jd.working_directory, jd.output)
        else:
            cobalt_params += '#COBALT --output %s\n' % jd.output

    if jd.error:
        # if working directory is set, we want stderr to end up in 
        # the working directory as well, unless it contains a specific
        # path name. 
        if jd.working_directory:
            if os.path.isabs(jd.error):
                cobalt_params += '#COBALT --error %s\n' % jd.error
            else:
                # user provided a realtive path for STDERR. in this case 
                # we prepend the workind directory path before passing
                # it on to Cobalt
                cobalt_params += '#COBALT --error %s/%s\n' \
                               % (jd.working_directory, jd.error)
        else:
            cobalt_params += '#COBALT --error %s\n' % jd.error

    if jd.wall_time_limit:
        hours = int(jd.wall_time_limit / 60)
        minutes = jd.wall_time_limit % 60
        cobalt_params += '#COBALT --time %s:%s:00\n' \
            % (str(hours).zfill(2), str(minutes).zfill(2))

    if queue:
        cobalt_params += '#COBALT --queue %s\n' % queue
    elif jd.queue:
        cobalt_params += '#COBALT --queue %s\n' % jd.queue

    if jd.project:
        cobalt_params += '#COBALT --project %s\n' % str(jd.project)

    if jd.job_contact:
        cobalt_params += '#COBALT --notify %s\n' % str(jd.job_contact)

    #
    # This section takes care of CPU/Process/Node calculation
    #
    # Handle number of cores
    # Default total_cpu_count = 1
    if jd.attribute_exists ("total_cpu_count"):
        total_cpu_count = jd.total_cpu_count
    else:
        logger.warning("total_cpu_count not specified -- default to (1)")
        total_cpu_count = 1

    # Request enough nodes to cater for the number of cores requested
    number_of_nodes = total_cpu_count / ppn
    if total_cpu_count % ppn > 0:
        number_of_nodes += 1

    # Get number of processes
    # Defaults to number_of_processes = number_of_nodes
    if jd.attribute_exists ("number_of_processes"):
        number_of_processes = jd.number_of_processes
    else:
        logger.debug("number_of_processes not specified. default: 1 per node")
        number_of_processes = number_of_nodes

    # Get number of processes per host/node
    # Defaults to processes_per_host = 1
    if jd.attribute_exists("processes_per_host"):
        processes_per_host = jd.processes_per_host
    else:
        logger.debug("processes_per_host not specified -- default to 1")
        processes_per_host = 1

    # Need to make sure that the 'processes_per_host' is a valid one
    # Blue Gene/Q valid modes ==> [1, 2, 4, 8, 16, 32, 64]
    # At the Blue Gene/Q, 1 Node == 16 Cores
    # and can handle UP TO 4 tasks per CPU/Core == 64 tasks
    #
    # References: 
    #   http://www.alcf.anl.gov/user-guides/cobalt-job-control
    #   https://www.alcf.anl.gov/user-guides/blue-geneq-versus-blue-genep
    if processes_per_host not in blue_gene_q_modes:
        log_error_and_raise("#processes per host %d incompatible with #nodes %d"
                           % (processes_per_host, blue_gene_q_modes),
                              rse.BadParameter, logger)

    # Make sure we aren't doing funky math
    # References:
    #   http://www.alcf.anl.gov/user-guides/machine-partitions
    #   the --proccount flag value must be <= nodecount * mode
    if  number_of_processes > (number_of_nodes * processes_per_host):
        log_error_and_raise (("number_of_processes (%d) must be <= to"
            "(number_of_nodes * processes_per_host) (%d * %d = %d)")
            % (number_of_processes, number_of_nodes, processes_per_host,
                (number_of_nodes * processes_per_host)), rse.NoSuccess, logger)

    # Other funky math checks should go here ~

    # Set number of nodes
    cobalt_params += '#COBALT --nodecount %d\n' % number_of_nodes

    # Set the total number of processes
    cobalt_params += '#COBALT --proccount %d\n' % number_of_processes

    # The Environments are added at the end because for now
    # Cobalt isn't supporting spaces in the env variables...
    # Which mess up the whole script if they are at the begining of the list...
    if jd.environment:
        cobalt_params += '#COBALT --env %s\n' % \
                ':'.join (["%s=%s" % (k,v.replace(':', '\\:')
                                         .replace('=', '\\='))  # escape chars
                           for k,v in jd.environment.items()])

    # Why do I need this?
    # Andre on Dev 16 2016: 
    # You don't, but it can be useful for some applications, 
    # so this is supposed to be available in the job environment for inspection.
    # This makes sense to be exported as an environment Variable
    cobalt_params += '#COBALT --env SAGA_PPN=%d\n' % ppn

    # may not need to escape all double quotes and dollarsigns, 
    # since we don't do 'echo |' further down (like torque/pbspro)
    # only escape '$' in args and exe. not in the params
    # exec_n_args = exec_n_args.replace('$', '\\$')

    # Set the MPI rank per node (mode).
    #   mode --> c1, c2, c4, c8, c16, c32, c64
    #   Mode is represented by the runjob's '--ranks-per-node' flag
    exec_n_args = ("%s --ranks-per-node %d --np %d --block $COBALT_PARTNAME"
                   "--verbose=INFO : %s\n") \
                % (run_job, processes_per_host, number_of_processes,
                   exec_n_args)
    exec_n_args = exec_n_args.replace('$', '\\$')

    # Need a new line before the shebang because linux is a bit of a pain when
    # echoing it.  It will be removed later though...
    cobaltscript = "\n#!/bin/bash \n%s\n%s" % (cobalt_params, exec_n_args)
    cobaltscript = cobaltscript.replace('"', '\\"')
    return cobaltscript


# ------------------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# ------------------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "radical.saga.adaptors.cobaltjob"
_ADAPTOR_SCHEMAS       = ["cobalt", "cobalt+ssh", "cobalt+gsissh"]
_ADAPTOR_OPTIONS       = [
    {
        'category'         : 'radical.saga.adaptors.cobaltjob',
        'name'             : 'base_workdir',
        'type'             : str,
        'default'          : "$HOME/.radical/saga/adaptors/cobaltjob/",
        'documentation'    : '''The adaptor stores job state information on the
                              filesystem on the target resource. This parameter
                              specified what location should be used.''',
        'env_variable'     : None
    },
    {
        'category'         : 'radical.saga.adaptors.cobaltjob',
        'name'             : 'purge_on_start',
        'type'             : bool,
        'default'          : True,
        'valid_options'    : [True, False],
        'documentation'    : '''Purge temporary job information for all
                              jobs which are older than a number of days.
                              The number of days can be configured with
                              <purge_older_than>.''',
        'env_variable'     : None
    },
    {
        'category'         : 'radical.saga.adaptors.cobaltjob',
        'name'             : 'purge_older_than',
        'type'             : int,
        'default'          : 30,
        'documentation'    : '''When <purge_on_start> is enabled this specifies
                                the number of days to consider a temporary file
                                older enough to be deleted.''',
        'env_variable'     : None
    },
]

# ------------------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES = {
    "jdes_attributes":   [api.NAME,
                          api.EXECUTABLE,
                          api.ARGUMENTS,
                          api.ENVIRONMENT,
                          api.INPUT,
                          api.OUTPUT,
                          api.ERROR,
                          api.QUEUE,
                          api.PROJECT,
                          api.WALL_TIME_LIMIT,
                          api.WORKING_DIRECTORY,
                          api.SPMD_VARIATION,  # TODO: 'hot'-fix for BigJob
                          api.PROCESSES_PER_HOST,
                          api.TOTAL_CPU_COUNT,
                          api.NUMBER_OF_PROCESSES,
                          api.JOB_CONTACT],
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

# ------------------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC = {
    "name":          _ADAPTOR_NAME,
    "cfg_options":   _ADAPTOR_OPTIONS,
    "capabilities":  _ADAPTOR_CAPABILITIES,
    "description":  """
The Cobalt adaptor allows to run and manage jobs on
`Cobalt <http://trac.mcs.anl.gov/projects/cobalt>`_controlled HPC clusters.
""",
    "example": "examples/jobs/cobaltjob.py",
    "schemas": {"cobalt":        "connect to a local cluster",
                "cobalt+ssh":    "connect to a remote cluster via SSH",
                "cobalt+gsissh": "connect to a remote cluster via GSISSH"}
}

# ------------------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA
#
_ADAPTOR_INFO = {
    "name"        : _ADAPTOR_NAME,
    "version"     : "v0.1",
    "schemas"     : _ADAPTOR_SCHEMAS,
    "capabilities": _ADAPTOR_CAPABILITIES,
    "cpis"        : [
                        {
                            "type": "radical.saga.job.Service",
                            "class": "CobaltJobService"
                        },
                        {
                            "type": "radical.saga.job.Job",
                            "class": "CobaltJob"
                        }
                    ]
}


###############################################################################

# The adaptor class
#
class Adaptor (a_base.Base):
    """ this is the actual adaptor class, which gets loaded by SAGA (i.e. by 
        the SAGA engine), and which registers the CPI implementation classes 
        which provide the adaptor's functionality.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        a_base.Base.__init__(self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile('^\[(.*)\]-\[(.*?)\]$')
        self.epoch = datetime.datetime(1970,1,1)

        # Adaptor Options
        self.base_workdir     = self._cfg['base_workdir']
        self.purge_on_start   = self._cfg['purge_on_start']
        self.purge_older_than = self._cfg['purge_older_than']

        self.base_workdir     = os.path.normpath(self.base_workdir)

        # dictionaries to keep track of certain Cobalt jobs data
        self._script_file         = dict()  # location of cobalt script file
        self._job_current_workdir = dict()  # working dir, for status checking


    # --------------------------------------------------------------------------
    #
    def sanity_check(self):
        # FIXME: also check for gsissh
        pass

    # --------------------------------------------------------------------------
    #
    def parse_id(self, id):
        # split the id '[rm]-[pid]' in its parts, and return them.

        match = self.id_re.match(id)

        if not match or len(match.groups()) != 2:
            raise rse.BadParameter("Cannot parse job id '%s'" % id)

        return (match.group(1), match.group(2))


###############################################################################
# CobaltJobService
class CobaltJobService (cpi_job.Service):
    """ implements cpi_job.Service
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        self._mt  = None
        _cpi_base = super(CobaltJobService, self)
        _cpi_base.__init__(api, adaptor)

        self._adaptor = adaptor

    # --------------------------------------------------------------------------
    #
    def __del__(self):

        self.close()


    # --------------------------------------------------------------------------
    #
    def close(self):

        if  self.mt :
            self.mt.stop()
            self.mt.join(10)  # don't block forever on join()

        self._logger.info("Job monitoring thread stopped.")

        self.finalize(True)


    # --------------------------------------------------------------------------
    #
    def finalize(self, kill_shell=False):

        if  kill_shell :
            if  self.shell :
                self.shell.finalize (True)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, rm_url, session):
        """ service instance constructor
        """
        self.rm      = rm_url
        self.session = session
        self.ppn     = 16       # DEFAULT MIRA -- BLUE GENE / Q IS 16 
        self.is_cray = ""
        self.queue   = None
        self.shell   = None
        self.jobs    = dict()
        self.gres    = None

        # the monitoring thread - one per service instance
        self.mt = _job_state_monitor(job_service=self)
        self.mt.start()

        rm_scheme = rm_url.scheme
        pty_url   = Url(rm_url)

        # this adaptor supports options that can be passed via the
        # 'query' component of the job service URL.
        if rm_url.query:
            for key, val in parse_qs(rm_url.query).items():
                if key == 'queue':
                    self.queue = val[0]
                # Disableing 'ppn' since Blue Gene/Q only supports 16 PPN
                # elif key == 'ppn':
                #     self.ppn = int(val[0])

        # we need to extract the scheme for PTYShell. That's basically the
        # job.Service Url without the pbs+ part. We use the PTYShell to execute
        # pbs commands either locally or via gsissh or ssh.
        if rm_scheme == "cobalt":
            pty_url.scheme = "fork"
        elif rm_scheme == "cobalt+ssh":
            pty_url.scheme = "ssh"
        elif rm_scheme == "cobalt+gsissh":
            pty_url.scheme = "gsissh"

        # these are the commands that we need in order to interact with Cobalt.
        # the adaptor will try to find them during initialize(self) and bail
        # out in case they are note available.
        self._commands = {'nodelist': None,
                          'partlist': None,
                          'qstat':    None,
                          'qsub':     None,
                          'qdel':     None,
                          'runjob':   None  # For running scripts
                          }

        self.shell = rsups.PTYShell(pty_url, self.session)

        # self.shell.set_initialize_hook(self.initialize)
        # self.shell.set_finalize_hook(self.finalize)

        self.initialize()
        return self.get_api()


    # --------------------------------------------------------------------------
    #
    def initialize(self):

        # Create the staging directory
        ret, out, _ = self.shell.run_sync ("mkdir -p %s"
                                                   % self._adaptor.base_workdir)
        if  ret != 0 :
            raise rse.NoSuccess("Error creating staging directory. (%s): (%s)"
                           % (ret, out))

        # Purge temporary files
        if self._adaptor.purge_on_start:
            cmd = "find %s -type f -mtime +%d -print -delete | wc -l" % (
                self._adaptor.base_workdir, 
                self._adaptor.purge_older_than
            )
            ret, out, _ = self.shell.run_sync(cmd)
            if ret == 0 and out != "0":
                self._logger.info("Purged %s temporary files" % out)

        # Check if all required cobalt tools are available
        for cmd in list(self._commands.keys()):
            ret, out, _ = self.shell.run_sync("which %s " % cmd)
            if ret != 0:
                message = "Error finding Cobalt tools: %s" % out
                log_error_and_raise(message, rse.NoSuccess, self._logger)
            else:
                path = out.strip()  # strip removes newline
                ret, out, _ = self.shell.run_sync("%s --version" % cmd)
                if ret != 0:
                    message = "Error finding Cobalt tools: %s" % out
                    log_error_and_raise(message, rse.NoSuccess,
                        self._logger)
                else:
                    # version is reported as
                    # "version: x.y.z" #.strip().split()[1]
                    version = out.strip().replace('\n', '')

                    # add path and version to the command dictionary
                    self._commands[cmd] = {"path": path, "version": version}
        self._logger.info("Found Cobalt tools: %s" % self._commands)


    # --------------------------------------------------------------------------
    #
    def _job_run(self, job_obj):
        """ runs a job via qsub
        """

        # Defaults ...
        cobalt_script_file      = None
        job_current_workdir     = '$HOME'

        # get the job description
        jd = job_obj.get_description()

        # normalize working directory path
        if  jd.working_directory :
            jd.working_directory = os.path.normpath (jd.working_directory)

        # TODO: Why would one want this?
        if self.queue and jd.queue:
            self._logger.warning("Job service was instantiated explicitly with \
                'queue=%s', but job description tries to a different queue: \
                '%s'. Using '%s'." %
                                (self.queue, jd.queue, self.queue))

        try:
            # create a Cobalt job script from SAGA job description
            script = _cobaltscript_generator(url=self.rm, 
                                   logger=self._logger, 
                                   jd=jd, 
                                   ppn=self.ppn, 
                                   queue=self.queue, 
                                   run_job=self._commands['runjob']['path']
                                  )
            self._logger.info("Generated Cobalt script: %s" % str(script))
        except Exception as ex:
            log_error_and_raise(str(ex), rse.BadParameter, self._logger)

        # try to create the working directory (if defined)
        # WARNING: this assumes a shared filesystem between login node and
        #          compute nodes.
        if jd.working_directory:
            self._logger.info("Create workdir %s" % jd.working_directory)
            ret, out, _ = self.shell.run_sync("mkdir -p %s"
                                                       % (jd.working_directory))
            job_current_workdir = jd.working_directory  # Keep track of the cwd, 
            if ret != 0:
                # something went wrong
                message = "Couldn't create working directory - %s" % (out)
                log_error_and_raise(message, rse.NoSuccess, self._logger)


        # Now we want to execute the script. This process consists of two steps:
        # (1) we create a temporary file with 'mktemp'
        #     in the 'self._adaptor.base_workdir' 
        #
        # (2) write the contents of 
        #     the generated Cobalt script into it, remove the first empty line
        #     and make sure it is executable
        #
        # (3) we call 'qsub --mode script <tmpfile>' to submit the script to
        #     the queueing system
        #
        self._logger.info("Creating Cobalt script file at %s"
                         % jd.working_directory)
        ret, out, _ = self.shell.run_sync("SCRIPTFILE=`mktemp -p %s \
                -t RS-PBSProJobScript.XXXXXX` && echo $SCRIPTFILE"
                % (self._adaptor.base_workdir))
        if ret != 0:
            message = "Couldn't create Cobalt script file - %s" % (out)
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        # Save Script file for later...
        # Cobalt *needs* the file to stick around, even after submission
        # so, we will keep the file around and delete it *only* when
        # the job is done.
        cobalt_script_file = out.strip()

        self._logger.info("script file: %s" % cobalt_script_file)
        ret, out, _ = self.shell.run_sync('SCRIPTFILE="%s" && echo "%s" \
                > $SCRIPTFILE && echo "$(tail -n +2 $SCRIPTFILE)" \
                > $SCRIPTFILE && chmod +x $SCRIPTFILE && echo $SCRIPTFILE'
                % (cobalt_script_file, script))
        if ret != 0:
            message = "Couldn't create Cobalt script file - %s" % (out)
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        cmdline = "%s --mode script %s" \
                % (self._commands['qsub']['path'], cobalt_script_file)
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
            self._logger.info("Submitted Cobalt job with id: %s" % job_id)

            state = api.PENDING

            # populate job info dict
            self.jobs[job_id] = {'obj'         : job_obj,
                                 'job_id'      : job_id,
                                 'state'       : state,
                                 'exec_hosts'  : None,
                                 'returncode'  : None,
                                 'create_time' : None,
                                 'start_time'  : None,
                                 'end_time'    : None,
                                 'gone'        : False
                                 }

            self._logger.info("assign job id  %s / %s / %s to watch list (%s)" 
                % (None, job_id, job_obj, list(self.jobs.keys())))

            # set status to 'pending' and manually trigger callback
            job_obj._attributes_i_set('state', state, job_obj._UP, True)

            # Since we now have the job_id, lets track the job's current
            # workdir and script file.  We do this in the adaptor
            self._adaptor._job_current_workdir[job_id]  = job_current_workdir
            self._adaptor._script_file[job_id]          = cobalt_script_file

            # return the job id
            return job_id


    # --------------------------------------------------------------------------
    #
    def _retrieve_job(self, job_id):
        """ see if we can get some info about a job that we don't
            know anything about
        """
        pass


    # --------------------------------------------------------------------------
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
                'job_name':     None,
                'exec_hosts':   None,
                'returncode':   None,
                'create_time':  None,
                'start_time':   None,
                'end_time':     None,
                'gone':         False
            }

        rm, pid = self._adaptor.parse_id(job_id)

        # run the Cobalt 'qstat' command to get some info about our job
        qstat_flag = '--full --long'
        ret, out, _ = self.shell.run_sync("unset GREP_OPTIONS; %s %s %s"
                      "| grep -E -i -e '^ *JobName ' "
                                  " -e '^ *QueuedTime ' "
                                  " -e '^ *RunTime ' "
                                  " -e '^ *Nodes ' "
                                  " -e '^ *Procs ' "
                                  " -e '^ *State ' "
                                  " -e '^ *Location ' "
                                  " -e '^ *StartTime ' "
                                  " -e '^ *SubmitTime ' "
                                  " -e '^ *S '"
                % (self._commands['qstat']['path'], qstat_flag, pid))

        if ret != 0:

            if reconnect:
                message = "Couldn't reconnect to job '%s': %s" % (job_id, out)
                log_error_and_raise(message, rse.NoSuccess, self._logger)

            if out.strip() == '':
                # Cobalt's 'qstat' command return's nothing
                # When a job is finished but it exists with code '1' 
                # Let's see the job's final state in the job's 'cobaltlog' file
                # which can be found at
                #        'self._adaptor._job_current_workdir_cwd/pid.cobaltlog' 
                # If file is found: get the final status of the job
                # If file not found: let's assume it FAILED. 

                # Run a 'cat' command to the final info about our job
                # Sample OUTPUT:
                # ...
                # Mon Jan 23 02:44:05 2017 +0000 (UTC) \
                # Info: task completed normally with an exit code of 126;\
                #       initiating job cleanup and removal

                ret, out, _ = self.shell.run_sync("unset GREP_OPTIONS; "
                              "cat %s/%s.cobaltlog "
                              "| grep -P -i '^[A-Z][a-z]{2} [A-Z][a-z]{2}"
                                             " \d{2} \d{2}:\d{2}:\d{2} \d{4}"
                                             " \+\d{4} \([A-Za-z]+\) *Info:"
                                             " task completed'"
                    % (self._adaptor._job_current_workdir[job_id], pid))

                if ret != 0:
                    if reconnect:
                        message = "Couldn't reconnect to job '%s': %s" % (job_id, out)
                        log_error_and_raise(message, rse.NoSuccess, self._logger)
                elif out.strip() == '':
                    # Let's see if the last known job state was running or pending. in
                    # that case, the job is gone now, which can either mean DONE,
                    # or FAILED. the only thing we can do is set it to 'DONE'
                    job_info['gone'] = True
                    # TODO: we can also set the end time?
                    self._logger.warning("Previously running job has disappeared. "
                            "This probably means that the backend doesn't store "
                            "information about finished jobs. Setting state to 'DONE'.")
                    if job_info['state'] in [api.RUNNING, api.PENDING]:
                        job_info['state'] = api.DONE
                    else:
                        # TODO: This is an uneducated guess?
                        job_info['state'] = api.FAILED
                else:
                    try:
                        # Found the cobaltlot file, let's grab the result...
                        matches = re.search('^([A-Z][a-z]{2} [A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2} \d{4} \+\d{4} \([A-Za-z]+\)) *Info: task completed .+ an exit code of (\d+);', out)
                        timestamp = matches.group(1).strip()
                        exit_code = matches.group(2).strip()
                    except Exception as e:
                        log_error_and_raise('Could not parse job status %s' % e,
                                            rse.NoSuccess, self._logger)

                    # Current format: Mon Jan 23 02:44:05 2017 +0000 (UTC)
                    # ASSUMPTION: Date is in UTC (as seen on the servers)
                    # Will be parsed as UTC and output format: 
                    # DDD mmm dd HH:MM:SS YYYY +0000 (UTC)
                    # Wed Dec 21 15:51:34 2016 +0000 (UTC)
                    end_time = datetime.datetime.strptime(timestamp,
                                             "%a %b %d %H:%M:%S %Y +0000 (UTC)")
                    job_info['end_time'] = (end_time - self._adaptor.epoch) \
                                                                .total_seconds()

                    # Return code is on position '13'
                    job_info['returncode'] = int(exit_code)

                    # Final Job State given the exit code
                    if job_info['returncode'] != 0:
                        job_info['state'] = api.FAILED
                    else:
                        job_info['state'] = api.DONE
            else:
                # something went wrong
                message = "Error retrieving job info via 'qstat': %s" % out
                log_error_and_raise(message, rse.NoSuccess, self._logger)
        else:

            # The job seems to exist on the system. let's process some data.

            # TODO: make the parsing "contextual", in the sense that it takes
            #       the state into account.

            # parse the egrep result. this should look something like this:
            #       QueuedTime        : 00:00:04
            #       RunTime           : 00:00:39
            #       Nodes             : 2
            #       State             : running
            #       Procs             : 3
            #       Location          : CENTOS-04000-37331-512
            #       StartTime         : Tue Nov 29 02:11:45 2016 +0000 (UTC)
            #       SubmitTime        : Tue Nov 29 02:11:40 2016 +0000 (UTC)
            #       S                 : R
            results = out.split('\n')
            for line in results:
                if len(line.split(':')) == 2:
                    key, val = line.split(':')
                    key = key.strip()
                    val = val.strip()

                    # The ubiquitous job state
                    if key in ['S']:  # Cobalt's PBS-like state
                        job_info['state'] = _cobalt_to_saga_jobstate(val)

                    # Hosts where the job ran
                    elif key in ['Location']:  # Cobalt's Node/Partition
                        # format CENTOS-04000-37331-512
                        job_info['exec_hosts'] = val

                    # Time job got created in the queue
                    elif key in ['SubmitTime']:
                        job_info['create_time'] = val

                    # Time job started to run
                    elif key in ['StartTime']:
                        job_info['start_time'] = val

                    # Job name
                    elif key in ['JobName']:
                        job_info['job_name'] = val


        # return the updated job info
        return job_info


    # --------------------------------------------------------------------------
    #
    def _parse_qstat(self, haystack, job_info):
        # return the new job info dict
        return job_info


    # --------------------------------------------------------------------------
    #
    def _job_get_state(self, job_id):
        """ get the job's state
        """
        return self.jobs[job_id]['state']


    # --------------------------------------------------------------------------
    #
    def _job_get_exit_code(self, job_id):
        """ get the job's exit code
        """
        ret = self.jobs[job_id]['returncode']

        # FIXME: 'None' should cause an exception
        if ret is None : return None
        else           : return int(ret)


    # --------------------------------------------------------------------------
    #
    def _job_get_execution_hosts(self, job_id):
        """ get the job's exit code
        """
        return self.jobs[job_id]['exec_hosts']


    # --------------------------------------------------------------------------
    #
    def _job_get_create_time(self, job_id):
        """ get the job's creation time
        """
        return self.jobs[job_id]['create_time']


    # --------------------------------------------------------------------------
    #
    def _job_get_start_time(self, job_id):
        """ get the job's start time
        """
        return self.jobs[job_id]['start_time']


    # --------------------------------------------------------------------------
    #
    def _job_get_end_time(self, job_id):
        """ get the job's end time
        """
        return self.jobs[job_id]['end_time']


    # --------------------------------------------------------------------------
    #
    def _job_cancel(self, job_id):
        """ cancel the job via 'qdel'
        """
        rm, pid = self._adaptor.parse_id(job_id)

        ret, out, _ = self.shell.run_sync("%s %s\n"
                                        % (self._commands['qdel']['path'], pid))

        if ret != 0:
            message = "Error canceling job via 'qdel': %s" % out
            log_error_and_raise(message, rse.NoSuccess, self._logger)

        # assume the job was succesfully canceled
        self.jobs[job_id]['state'] = api.CANCELED


    # --------------------------------------------------------------------------
    #
    def _job_wait(self, job_id, timeout):
        """ wait for the job to finish or fail
        """
        time_start = time.time()
        time_now   = time_start
        rm, pid    = self._adaptor.parse_id(job_id)

        while True:
            state = self.jobs[job_id]['state']  # this gets updated in the bg.

            if state in [api.DONE, api.FAILED, api.CANCELED]:
                return True

            # avoid busy poll
            time.sleep(SYNC_WAIT_UPDATE_INTERVAL)

            # check if we hit timeout
            if timeout >= 0:
                time_now = time.time()
                if time_now - time_start > timeout:
                    return False


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def create_job(self, jd):
        """ implements cpi_job.Service.get_url()
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


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job(self, job_id):
        """ Implements cpi_job.Service.get_job()

            Re-create job instance from a job-id.
        """

        # If we already have the job info, we just pass the current info.
        if job_id in self.jobs :
            return self.jobs[job_id]['obj']

        # Try to get some initial information about this job (again)
        job_info = self._job_get_info(job_id, reconnect=True)

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         # TODO: fill job description
                         "job_description": api.Description(),
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


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url(self):
        """ implements cpi_job.Service.get_url()
        """
        return self.rm


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self):
        """ implements cpi_job.Service.list()
        """
        ids = []

        ret, out, _ = self.shell.run_sync("unset GREP_OPTIONS; %s "
            "--header=JobID:JobName:User:Walltime:RunTime:Nodes:State:"
            "short_state:Location:Queue | grep `whoami`"
            % self._commands['qstat']['path'])

        if ret != 0 and len(out) > 0:
            message = "failed to list jobs via 'qstat': %s" % out
            log_error_and_raise(message, rse.NoSuccess, self._logger)
        elif ret != 0 and len(out) == 0:
            # qstat | grep `` exits with 1 if the list is empty
            pass
        else:
            for line in out.split("\n"):
                # output looks like this: 
                # (not inlcuding the first line - that is for reference only)
                #
                # JobID  JobName  User     Walltime  RunTime   Nodes  State      S  Location                Queue
                # 32     hello    vagrant  00:50:00  00:00:00  2      starting   R  CENTOS-04400-37731-512  default  
                # 33     hello    vagrant  00:50:00  00:00:11  2      running    R  CENTOS-04440-37771-512  default  
                # 35     hello    vagrant  00:50:00  N/A       4      queued     Q  None                    default  
                # 36     hello    vagrant  00:50:00  N/A       2      queued     Q  None                    default  
                # 37     hello    vagrant  00:50:00  00:00:22  2      running    R  CENTOS-04040-37371-512  default  
                # 38     hello    vagrant  00:50:00  N/A       2      queued     Q  None                    default  
                # 40     hello    vagrant  00:50:00  N/A       2      user_hold  H  None                    default 
                # 41     hello    vagrant  00:50:00  N/A       1      queued     Q  None                    default  
                if len(line.split()) > 1:
                    job_id = "[%s]-[%s]" % (self.rm, line.split()[0].strip())
                    ids.append(str(job_id))
        return ids


    # --------------------------------------------------------------------------
    #
    def container_run (self, jobs) :
        self._logger.debug ("container run: %s"  %  str(jobs))
        # TODO: this is not optimized yet
        for job in jobs:
            job.run ()


    # --------------------------------------------------------------------------
    #
    def container_wait (self, jobs, mode, timeout) :
        self._logger.debug ("container wait: %s"  %  str(jobs))
        # TODO: this is not optimized yet
        for job in jobs:
            job.wait ()


    # --------------------------------------------------------------------------
    #
    def container_cancel (self, jobs, timeout) :
        # TODO: this is not optimized yet
        for job in jobs:
            job.cancel(timeout)


###############################################################################
#
class CobaltJob (cpi_job.Job):
    """ implements cpi_job.Job
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        # initialize parent class
        _cpi_base = super(CobaltJob, self)
        _cpi_base.__init__(api, adaptor)


    # --------------------------------------------------------------------------
    #
    def _get_impl(self):
        return self


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, job_info):
        """ implements cpi_job.Job.init_instance()
        """
        # init_instance is called for every new rs.job.Job object
        # that is created
        self.jd = job_info["job_description"]
        self.js = job_info["job_service"]

        if job_info['reconnect'] is True:
            self._id      = job_info['reconnect_jobid']
            self._name    = self.jd.get(api.NAME)
            self._started = True
        else:
            self._id      = None
            self._name    = self.jd.get(api.NAME)
            self._started = False

        return self.get_api()


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        """ implements cpi_job.Job.get_state()
        """
        if  self._started is False:
            return api.NEW

        return self.js._job_get_state(job_id=self._id)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def wait(self, timeout):
        """ implements cpi_job.Job.wait()
        """
        if self._started is False:
            log_error_and_raise("Can't wait for job that hasn't been started",
                rse.IncorrectState, self._logger)
        else:
            self.js._job_wait(job_id=self._id, timeout=timeout)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):
        """ implements cpi_job.Job.cancel()
        """
        if self._started is False:
            log_error_and_raise("Can't wait for job that hasn't been started",
                rse.IncorrectState, self._logger)
        else:
            self.js._job_cancel(self._id)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):
        """ implements cpi_job.Job.run()
        """
        self._id = self.js._job_run(self._api())
        self._started = True


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_service_url(self):
        """ implements cpi_job.Job.get_service_url()
        """
        return self.js.rm


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id(self):
        """ implements cpi_job.Job.get_id()
        """
        return self._id


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_name (self):
        """ Implements cpi_job.Job.get_name() """        
        return self._name


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code(self):
        """ implements cpi_job.Job.get_exit_code()
        """
        if not self._started:
            return None
        else:
            return self.js._job_get_exit_code(self._id)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created(self):
        """ implements cpi_job.Job.get_created()
        """
        if self._started is False:
            return None
        else:
            # FIXME: convert to EPOCH
            return self.js._job_get_create_time(self._id)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started(self):
        """ implements cpi_job.Job.get_started()
        """
        if self._started is False:
            return None
        else:
            # FIXME: convert to EPOCH
            return self.js._job_get_start_time(self._id)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished(self):
        """ implements cpi_job.Job.get_finished()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_end_time(self._id)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_execution_hosts(self):
        """ implements cpi_job.Job.get_execution_hosts()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_execution_hosts(self._id)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_description(self):
        """ implements cpi_job.Job.get_execution_hosts()
        """
        return self.jd


# ------------------------------------------------------------------------------

