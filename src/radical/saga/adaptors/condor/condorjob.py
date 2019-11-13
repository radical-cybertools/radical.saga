
__author__    = "Andre Merzky, Mark Santcroos, Ole Weidner"
__copyright__ = "Copyright 2012-2015, The SAGA Project"
__license__   = "MIT"


""" Condor job adaptor implementation
"""

import re
import os
import time
import datetime

from urllib.parse import parse_qs
from tempfile import NamedTemporaryFile

import radical.utils as ru

from .. import base
from .. import cpi

from ...exceptions    import *
from ...job.constants import *
from ...exceptions    import *
from ...              import job        as api
from ...              import filesystem as sfs
from ...utils         import pty_shell  as sups
from ...utils.job     import TransferDirectives

SYNC_CALL  = cpi.decorators.SYNC_CALL
ASYNC_CALL = cpi.decorators.ASYNC_CALL


# --------------------------------------------------------------------
#
def log_error_and_raise(message, exception, logger):
    logger.error(message)
    raise exception(message)


# --------------------------------------------------------------------
#
def _condor_to_saga_jobstate(condorjs):
    """ translates a condor one-letter state to saga
    """
    # From: http://pages.cs.wisc.edu/~adesmet/status.html
    #
    # JobStatus in job ClassAds
    #
    # 0   Unexpanded      U
    # 1   Idle            I
    # 2   Running         R
    # 3   Removed         X
    # 4   Completed       C
    # 5   Held            H
    # 6   Submission_err  E

    if   int(condorjs) == 0: return api.PENDING
    elif int(condorjs) == 1: return api.PENDING
    elif int(condorjs) == 2: return api.RUNNING
    elif int(condorjs) == 3: return api.CANCELED
    elif int(condorjs) == 4: return api.DONE
    elif int(condorjs) == 5: return api.PENDING
    elif int(condorjs) == 6: return api.FAILED
    else                   : return api.UNKNOWN


# --------------------------------------------------------------------
#
def _condorscript_generator(url, logger, jds, option_dict=None):
    """
    generates a Condor script from a set of SAGA job descriptions
    """

    if not isinstance(jds, list):
        jds = [jds]

    # assert that all jds belong to the same project (which might be empty)
    project = jds[0].project
    for jd in jds:
        assert(project == jd.project)


    condor_file = str()

    # HTCondor quoting/escaping:
    # http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html#SECTION0012514000000000000000

    ##### options passed via job service url #####
    if project or option_dict:
        condor_file += "\n##### OPTIONS PASSED VIA JOB SERVICE URL #####\n"

        if project:
            condor_file += "\n+ProjectName = \"%s\"" % jd.project

        if option_dict:
            for key,value in option_dict.items():
                condor_file += "\n%s = %s" % (key, value)


    ##### options passed via job description #####
    condor_file += "\n\n##### OPTIONS PASSED VIA JOB DESCRIPTION #####\n"

    # Per Job in Bulk settings
    for jd in jds:

        # environment -> environment
        # http://research.cs.wisc.edu/htcondor/manual/current/condor_submit.html#SECTION0012514000000000000000
        if jd.environment:
            environment = ''
            for key,val in jd.environment.items():
                environment += ' "%s=%s"'     % (key, val)
              # condor_file += "\n%-25s = %s" % (key, val)
            condor_file += "\nenvironment               = %s\n" % environment

        condor_file += "\nstream_output             = True"
        condor_file += "\nstream_error              = True"
        condor_file += "\nshould_transfer_files     = YES"
        condor_file += "\nwhen_to_transfer_output   = ON_EXIT_OR_EVICT\n"


        # handle site inclusion/exclusion
        requirements = "(NumJobStarts =?= 0 || NumJobStarts =?= Undefined)"
      # requirements = "(Target.MAX.PREEMPT >= 24*60*60) || (Target.MAX.PREEMPT == false)"

        if jd.candidate_hosts:

            # special characters / strings
            # TODO: this is bound to GLIDEIN which it shouldnt be.
            EXCLUSION    = '!'
            REQUIRED     = '~'
            KEY          = 'GLIDEIN_ResourceName'

            # Whitelist sites, filter out "special" entries from the candidate
            # host lists
            excl_sites = [host for host in jd.candidate_hosts
                               if  host.startswith(EXCLUSION)]
            req_sites  = [host for host in jd.candidate_hosts
                               if  host.startswith(REQUIRED)]
            incl_sites = [host for host in jd.candidate_hosts
                               if not host.startswith((REQUIRED, EXCLUSION))]

            if incl_sites:
                requirements += ' && (' +  ' || '.join(['%s =?= "%s"' % (KEY, site)
                                for site in incl_sites]) + ')'

            # Blacklist sites, strip out leading '!'
            if excl_sites:
                requirements += ' && (' +  ' && '.join(['%s =!= "%s"' % (KEY, site[1:])
                        for site in excl_sites]) + ')'

            # Get the '~special_requirements' and strip leading ~
            if req_sites:
                requirements += ' && '.join(req_sites)

        condor_file += "\nrequirements    = %s" % requirements
        condor_file += "\nperiodic_remove = NumJobStarts > 0 && JobStatus == 1\n"


        # special treatment for universe - defaults to 'vanilla'
        if jd.queue:
            condor_file += "\nuniverse     = %s" % jd.queue
        else:
            condor_file += "\nuniverse     = vanilla"

        # Condor doesn't expand environment variables in arguments.
        # To support this functionality, we wrap by calling
        #     /bin/env /bin/sh -c ""
        # where the arg string to the shell gets expanded
        condor_file += "\nexecutable   = /bin/env"

        # the actual executable becomes the first argument.
        if not jd.executable:
            log_error_and_raise("Executable not set", NoSuccess, logger)
        arguments = "%s" % jd.executable

        # all other arguments follow
        if jd.arguments:

            for arg in jd.arguments:

                # Condor can't deal with multi-line arguments. yep, that's how
                # bad of a software it is.
                if '\n' in arg:
                    message = "Condor doesn't support multi-line arguments: %s" % arg
                    log_error_and_raise(message, NoSuccess, logger)

                # Condor HATES double quotes in the arguments. It'll return
                # some crap like: "Found illegal unescaped double-quote: ...
                # That's why we escape them by repeating.
                arg = arg.replace('"', '""')
                arg = arg.replace("'", "''")

                # Escape dollars (for environment variables)
                #arg = arg.replace('$', '\\$')

                arguments += " %s" % arg

        # arguments -> arguments
        condor_file += "\narguments    = \"/bin/sh -c '%s'\"" % arguments

        # Transfer Directives
        assert(jd.transfer_directives)

        # all checking is done in _handle_file_transfers() already.
        condor_file += "\n"
        td = jd.transfer_directives

        if td.transfer_output_files:
            condor_file += "\ntransfer_output_files = %s" \
                         % ','.join(td.transfer_output_files)

        if td.transfer_input_files:
            condor_file += "\ntransfer_input_files  = %s\n" \
                         % ','.join(td.transfer_input_files)

        # TODO: what to do when working directory is not set?
        job_pwd = './'
        if jd.working_directory:
            job_pwd = jd.working_directory
            condor_file += "\ninitialdir   = %s " % job_pwd
        logname = "radical-saga-job-$(cluster)_$(process).log"
        condor_file += "\nlog          = %s " % os.path.join(job_pwd, logname)

        # output -> output
        if jd.output is not None:
            condor_file += "\noutput       = %s " % os.path.join(job_pwd, jd.output)

        # error -> error
        if jd.error is not None:
            condor_file += "\nerror        = %s " % os.path.join(job_pwd, jd.error)

        if jd.total_cpu_count:
            condor_file += "\nrequest_cpus = %d" % jd.total_cpu_count


        # 'queue' concludes the description of a job
        condor_file += "\nqueue\n"
        condor_file += "\n##### END OF JOB #####\n\n"

    condor_file += "\n##### END OF FILE #####\n\n"

    return condor_file


# --------------------------------------------------------------------
# some private defs
#
_CACHE_TIMEOUT = 5.0

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "radical.saga.adaptors.condorjob"
_ADAPTOR_SCHEMAS       = ["condor", "condor+ssh", "condor+gsissh"]

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES = {
    "jdes_attributes":   [NAME,
                          EXECUTABLE,
                          ARGUMENTS,
                          ENVIRONMENT,
                          INPUT,
                          OUTPUT,
                          ERROR,
                          QUEUE, # TODO: map jd.queue to universe or pool?!
                          PROJECT,
                          WALL_TIME_LIMIT,
                          WORKING_DIRECTORY,
                          CANDIDATE_HOSTS,
                          TOTAL_CPU_COUNT,
                          PROCESSES_PER_HOST,
                          SPMD_VARIATION,
                          FILE_TRANSFER],
    "job_attributes":    [EXIT_CODE,
                          EXECUTION_HOSTS,
                          NAME,
                          CREATED,
                          STARTED,
                          FINISHED],
    "metrics":           [STATE],
    "contexts":          {"ssh": "SSH public/private keypair",
                          "x509": "GSISSH X509 proxy context",
                          "userpass": "username/password pair (ssh)"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC = {
    "name":          _ADAPTOR_NAME,
    "capabilities":  _ADAPTOR_CAPABILITIES,
    "description":  """
The (HT)Condor(-G) adaptor allows to run and manage jobs on a
`Condor <http://research.cs.wisc.edu/htcondor/>`_ gateway.
""",
    "example": "examples/jobs/condorjob.py",
    "schemas": {"condor"        : "connect to a local gateway",
                "condor+ssh"    : "connect to a remote gateway via SSH",
                "condor+gsissh ": "connect to a remote gateway via GSISSH"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA
#
_ADAPTOR_INFO = {
    "name"             : _ADAPTOR_NAME,
    "version"          : "v0.2",
    "schemas"          : _ADAPTOR_SCHEMAS,
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "cpis": [
        {
        "type"         : "radical.saga.job.Service",
        "class"        : "CondorJobService"
        },
        {
        "type"         : "radical.saga.job.Job",
        "class"        : "CondorJob"
        }
    ]
}


###############################################################################
# The adaptor class
class Adaptor (base.Base):
    """ this is the actual adaptor class, which gets loaded by SAGA (i.e. by
        the SAGA engine), and which registers the CPI implementation classes
        which provide the adaptor's functionality.
    """

    # ----------------------------------------------------------------
    #
    def __init__(self):

        base.Base.__init__(self, _ADAPTOR_INFO)

        self.id_re    = re.compile('^\[(.*)\]-\[(.*?)\]$')
        self.use_hist = self._cfg.get('use_history', False)
        self.epoch    = datetime.datetime(1970,1,1)

        self._logger.info("use condor_history: %s", self.use_hist)


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
            raise BadParameter("Cannot parse job id '%s'" % id)

        return (match.group(1), match.group(2))


###############################################################################
#
class CondorJobService (cpi.job.Service):
    """ implements cpi.job.Service
    """

    # ----------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        _cpi_base = super(CondorJobService, self)
        _cpi_base.__init__(api, adaptor)

        self._adaptor = adaptor


    # ----------------------------------------------------------------
    #
    def __del__(self):

        self.finalize(kill_shell=True)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, rm_url, session):
        """ service instance constructor
        """
        self.rm            = rm_url
        self.session       = session
        self.ppn           = 0
        self.is_cray       = False
        self.jobs          = dict()
        self.query_options = dict()

        rm_scheme = rm_url.scheme
        pty_url   = ru.Url (rm_url)

        # this adaptor supports options that can be passed via the
        # 'query' component of the job service URL.
        if rm_url.query is not None:
            for key, val in parse_qs(rm_url.query).items():
                self.query_options[key] = val[0]

        # we need to extract the scheme for PTYShell. That's basically the
        # job.Service Url without the condor+ part. We use the PTYShell to
        # execute condor commands either locally or via gsissh or ssh.
        if rm_scheme == "condor":
            pty_url.scheme = "fork"
        elif rm_scheme == "condor+ssh":
            pty_url.scheme = "ssh"
        elif rm_scheme == "condor+gsissh":
            pty_url.scheme = "gsissh"

        # these are the commands that we need in order to interact with Condor.
        # the adaptor will try to find them during initialize(self) and bail
        # out in case they are not available.
        self._commands = {'condor_version': None,
                          'condor_submit':  None,
                          'condor_q':       None,
                          'condor_history': None,
                          'condor_vacate':  None,
                          'condor_rm':      None}

        self.shell = sups.PTYShell(pty_url, self.session)

      # self.shell.set_initialize_hook(self.initialize)
      # self.shell.set_finalize_hook(self.finalize)

        self.initialize()

        return self.get_api ()


    # ----------------------------------------------------------------
    #
    def close (self) :
        if  self.shell :
            self.shell.finalize (True)


    # ----------------------------------------------------------------
    #
    def initialize(self):

        # check if all required condor tools are available
        commands = list(self._commands.keys())
        ret, out, _ = self.shell.run_sync("which %s " % ' '.join(commands))

        if ret != 0:
            message = "Error finding Condor tools: %s" % out
            log_error_and_raise(message, NoSuccess, self._logger)

        # split and remove empty lines
        lines = list(filter(bool, out.split('\n')))

        if len(lines) != len(commands):
            message = "Error finding some Condor tools: %s" % out
            log_error_and_raise(message, NoSuccess, self._logger)

        for cmd, path in zip(commands, lines):
            self._commands[cmd] = path.strip()

          # if cmd == 'condor_version':
          #     ret, out, _ = self.shell.run_sync("%s" % cmd)
          #     if ret != 0:
          #         message = "Error determining Condor version: %s" % out
          #         log_error_and_raise(message, NoSuccess,
          #             self._logger)
          #     else:
          #         # version is reported as:
          #         # $CondorVersion: 7.8.6 Oct 25 2012 $
          #         # $CondorPlatform: X86_64-CentOS_5.7 $
          #         lines = out.split('\n')
          #         version = lines[0].replace("$CondorVersion: ", "")
          #         version = version.strip(" $")
          #
          #         # add path and version to the command dictionary
          # self._commands[cmd] = {"path":    path}
          #                        "version": version}

        self._logger.info("Found Condor tools: %s", self._commands)

    # ----------------------------------------------------------------
    #
    def finalize(self, kill_shell=False):
        pass


    # --------------------------------------------------------------------------
    #
    # condor_q can fail due to the backend timing out, or other backend errors
    # in general.  We thus use this wrapper around condor_q to retry a couple of
    # times in those events.
    #
    # TODO: make the decision to retry somewhat more clever, like only on
    #       timeouts etc.
    #
    def _run_condor_q(self, retries=1, timeout=10,
                      pid=None, options=None, egrep=None):
        ret = None
        out = ""
        err = ""
        cmd = 'unset GREP_OPTIONS; %s' % self._commands['condor_q']

        if pid:     cmd += ' -long %s'       % pid
        if options: cmd += ' %s'             % options

        if egrep:
            cmd += " | grep"
            for expr in egrep:
                cmd += " -e '%s'" % expr

        # we  try at most 'retries' times - but the loop may exit early
        for n in range(retries):

            # if we tried before wait for a little
            if ret != None and timeout:
                time.sleep(timeout)

            _ret, _out, _err = self.shell.run_sync(cmd)
            if _ret == 0:
                # this one worked - ignore any previous runs (if any)
                # and record the result
                ret = _ret
                out = _out
                err = _err
                break

            else:
                # this one failed - record error and retval
                ret  = _ret
                out += '\n\nretry %d:\n%s' % (n, _out)
                err += '\n\nretry %d:\n%s' % (n, _err)

                self._logger.debug('need retry %s', options)

                # if pid was given, then failur means job not found.  No point
                # retrying...
                if pid:
                    break

        # we exhausted the number of retries, or we succeeded - either way,
        # return what we have.  This will return the most recent retval, and
        # output.  In the case of repeated errors, the output contains
        # a *concatenation* of error messages from the different tries.
        return ret, out, err


    # ----------------------------------------------------------------
    #
    def _new_job_info(self):
        return {'state':        UNKNOWN,
                'exec_hosts':   None,
                'returncode':   None,
                'create_time':  None,
                'start_time':   None,
                'end_time':     None,
                'gone':         False,
                'td':           TransferDirectives(),
                'stdout':       None,
                'stderr':       None,
                'name':         None,
                'timestamp':    0.0
            }

    # --------------------------------------------------------------------------
    #
    def _prepare_jd(self, jd):

        if not jd.file_transfer:
            jd.file_transfer = []

        # Our submitted Condor script wraps the executable into a /bin/sh
        # command, so Condor will not be able to infer the actual executable,
        # and thus will make no attempt at staging it to the target host --
        # which is what Condor usually does.
        #
        # To recover that behavior, we add the respective file transfer
        # directive for the second hop (first hop: laptop -> submission host,
        # second hop : submission host -> execution site).  However, we only add
        # that for executables starting with './', as we can't stage to other
        # locations than the job's workdir anyway.
        #
        # If a SAGA application wants to stage the executable over the *first*
        # hop, ie. to the submission host, it has to explicitly specify that in
        # the job description -- just like SAGA requires for all other backends.
        if jd.executable.startswith('./'):

            exe = jd.executable

            # Check if the executable is already in the file_transfer list,
            # because then we don't need to implicitly add it anymore.
            # (For example, if the executable is not in the local directory, it needs
            # to be explicitly added.)
            stage_exe = True
            for ft in jd.file_transfer:
                if ft.endswith(exe):
                    stage_exe = False

            if stage_exe:
                # staging from submission site to execution host is signalled by
                # using a 'site:' prefix in the staging directive'
                exe_transfer = 'site:%s > %s' % (exe, exe)
                jd.file_transfer.append(exe_transfer)

        # translate file transfer directives into actionables
        jd.transfer_directives = TransferDirectives(jd.file_transfer)


    # ----------------------------------------------------------------
    #
    def _job_run(self, jd):
        """ runs a job via condor_submit
        """

        # ensure consistency and viability of job description
        self._prepare_jd(jd)
        self._handle_file_transfers(jd.transfer_directives, mode='in')

        # create a Condor job script from SAGA job description
        script = _condorscript_generator(url=self.rm, logger=self._logger,
                jds=[jd], option_dict=self.query_options)
        self._logger.info("Generated Condor script: %s", script)

        submit_file = NamedTemporaryFile(mode='w', suffix='.condor',
                    prefix='tmp-radical-saga-', delete=False)
        submit_file_name = os.path.basename(submit_file.name)
        submit_file.write(script)
        submit_file.close()
        self._logger.info("Written Condor script locally: %s", submit_file.name)

        if self.shell.url.scheme not in ["ssh", "gsissh"]:
            raise NotImplementedError("%s support for Condor not implemented." % \
                    self.shell.url.scheme)

        self._logger.info("Transferring Condor script to: %s", self.shell.url)
        self.shell.stage_to_remote(submit_file.name, submit_file_name)

        ret, out, _ = self.shell.run_sync('%s -verbose %s' \
            % (self._commands['condor_submit'], submit_file_name))

        if ret != 0:
            # something went wrong
            message = "Error running job via 'condor_submit': %s. Script was: %s" \
                % (out, script)
            log_error_and_raise(message, NoSuccess, self._logger)
        else:
            # stdout contains the job id
            for line in out.split("\n"):
                if "** Proc" in line:
                    pid = line.split()[2][:-1]
                    break

            # we don't want the 'query' part of the URL to be part of the ID,
            # simply because it can get terribly long (and ugly). to get rid
            # of it, we clone the URL and set the query part to None.
            rm_clone = ru.Url (self.rm)
            rm_clone.query = ""
            rm_clone.path  = ""

            job_id = "[%s]-[%s]" % (rm_clone, pid)
            self._logger.info("Submitted Condor job with id: %s", job_id)

            # add job to internal list of known jobs.
            self.jobs[job_id]          = self._new_job_info()
            self.jobs[job_id]['state'] = PENDING
            self.jobs[job_id]['td']    = jd.transfer_directives

            # remove submit file(s)
            # TODO: leave them in case of debugging?
            self._logger.info("Submitted Condor job with scheme: '%s'", self.shell.url.scheme)
          # if self.shell.url.scheme in ['ssh', 'gsissh']:
          #     ret, out, _ = self.shell.run_sync ('rm %s' % submit_file_name)
          # else:
          #     raise NotImplementedError("%s support for Condor not implemented." % \
          #                      self.shell.url.scheme)
            os.remove(submit_file.name)

            return job_id

    # ----------------------------------------------------------------
    #
    def _retrieve_job(self, job_id):
        """ see if we can get some info about a job that we don't
            know anything about
        """
        rm, pid = self._adaptor.parse_id(job_id)

        # run the Condor 'condor_q' command to get some infos about our job
        ret, out, err = self._run_condor_q(retries=3, timeout=60, pid=pid,
                egrep=['^JobStatus', '^ExitStatus', '^CompletionDate'])

        if ret != 0:
            message = "Couldn't reconnect to job '%s':\n[%s]\n[%s]" % \
                    (job_id, out, err)
            log_error_and_raise(message, NoSuccess, self._logger)

        else:
            # the job seems to exist on the backend. let's gather some data
            job_info = self._new_job_info()

            results = out.split('\n')
            for result in results:
                if len(result.split('=')) == 2:
                    key, val = result.split('=')
                    key = key.strip()  # strip() removes whitespaces at the
                    val = val.strip()  # beginning and the end of the string

                    if   key == 'JobStatus'     : job_info['state']      = _condor_to_saga_jobstate(val)
                    elif key == 'ExitStatus'    : job_info['returncode'] = val
                    elif key == 'CompletionDate': job_info['end_time']   = val

            return job_info


    # --------------------------------------------------------------------------
    #
    def _handle_file_transfers(self, td, mode):
        """
        if mode == 'in' : perform sanity checks on all staging directives.

        if mode == 'in' : stage files to   condor submission site
        if mode == 'out': stage files from condor submission site
        """

        assert(mode in ['in', 'out'])

        # Condor data staging happens in two hops, a fact that is not reflected
        # in SAGA's file transfer directives.  The hops are:
        #
        #   1: from the application host to the condor submission site
        #   2: from the condor submission site to the execution site
        #
        # We introduce a condor specific notion to avoid the first hop on
        # staging, for those cases where we expect the files to already reside
        # on the submission site, eg. due to out-of-band staging etc.  In those
        # cases, we prefix the respective input staging source with 'site:'.
        # That prefix is also evaluated for targets of output staging, where it
        # causes the files to transfered only over the second hop, but not
        # further back to the application host.
        #
        # The code below filters for those prefixes, and will enact
        # 'stage_to_remote' and 'stage_from_remote' only for entries w/o the
        # respective prefixes.  The prefixes are removed, so that the condor
        # submission file contains clean staging directives for hop 2.

        if mode == 'in':

            td.prepared = True
            td.transfer_input_files  = []
            td.transfer_output_files = []

            # Condor specific safety checks
            if td.in_append:
                raise Exception('File append (>>) not supported')

            if td.out_append:
                raise Exception('File append (<<) not supported')

            if td.in_overwrite:

                for (local, remote) in td.in_overwrite:

                    source = local
                    target = remote

                    hop_1 = True
                    if source.startswith('site:'):
                        source = source[5:]
                        hop_1  = False

                    (s_path, s_entry) = os.path.split(source)
                    (t_path, t_entry) = os.path.split(target)

                    # make sure source is file an not dir
                    if not s_entry:
                        # TODO: this doesn't seem true, but dont want to tackle it right now
                        raise Exception('directory staging not supported: %s' % source)

                    # make sure source and target file are the same
                    if s_entry != t_entry:
                        raise Exception('source and target names must be equal: %s != %s' % (s_entry, t_entry))

                    # add for later use by job script generator
                    td.transfer_input_files.append(source)

                    if hop_1 and self.shell.url.scheme in ["ssh", "gsissh"]:
                        self._logger.info("Transferring in %s to %s", source, target)
                        self.shell.stage_to_remote(source, target,
                                                   cp_flags=sfs.CREATE_PARENTS)

            if td.out_overwrite:

                for (local, remote) in td.out_overwrite:

                    source = remote
                    target = local

                    (s_path, s_entry) = os.path.split(source)
                    (t_path, t_entry) = os.path.split(target)

                    if target.startswith('site:'):
                        target = target[5:]

                    # make sure source is file and not dir
                    if not s_entry:
                        # TODO: this doesn't seem true, but dont want to tackle it right now
                        raise Exception('directory staging not supported: %s' % source)

                    # make sure source and target file are the same
                    if s_entry != t_entry:
                        raise Exception('source and target names must be equal: %s != %s' % (s_entry, t_entry))

                    # add for later use by job script generator
                    td.transfer_output_files.append(t_entry)


        # ----------------------------------------------------------------------
        elif mode == 'out':

            # make sure mode=='in' has been used before
            if not hasattr(td, 'prepared'):
                # we don't have that attribute, its likely a cloned job
                # description or something.  Warn and bail out
                self._logger.warning('unprepared for output staging')
                return

            assert(td.prepared)

            if td.out_overwrite:

                for (local, remote) in td.out_overwrite:

                    source = remote
                    target = local

                    hop_1 = True
                    if target.startswith('site:'):
                        target = target[5:]
                        hop_1  = False

                    (s_path, s_entry) = os.path.split(source)
                    (t_path, t_entry) = os.path.split(target)

                    if hop_1 and self.shell.url.scheme in ["ssh", "gsissh"]:
                        self._logger.info("Transferring out %s to %s", source, target)
                        self.shell.stage_from_remote(source, target,
                                                     cp_flags=sfs.CREATE_PARENTS)


    # ----------------------------------------------------------------
    #
    def _job_get_info(self, job_id):
        """ get job attributes via condor_q
        """

        # if we don't have the job in our dictionary, we don't want it
        if job_id not in self.jobs:
            message = "Unknown job ID: %s. Can't update state." % job_id
            log_error_and_raise(message, NoSuccess, self._logger)

        info = self.jobs[job_id]

        # if the 'gone' flag is set, there's no need to query the job
        # state again. it's gone forever
        if info['gone'] is True:
            return info

        # if we just queried the job info, don't query again
        if time.time() - info['timestamp'] < _CACHE_TIMEOUT:
            return info

        rm, pid = self._adaptor.parse_id(job_id)

        # run the Condor 'condor_q' command to get some infos about our job
        ret, out, err = self._run_condor_q(retries=3, timeout=60, pid=pid,
                egrep=['^JobStatus', '^ExitStatus', '^CompletionDate'])

        if ret == 0:

            # parse the egrep result. this should look something like this:
            # JobStatus = 5
            # ExitStatus = 0
            # CompletionDate = 0
            results = list(filter(bool, out.split('\n')))
            for result in results:
                key, val = result.split('=', 1)
                key = key.strip()
                val = val.strip()

                if   key == 'JobStatus'     : info['state']      = _condor_to_saga_jobstate(val)
                elif key == 'ExitStatus'    : info['returncode'] = val
                elif key == 'CompletionDate': info['end_time']   = val

        else:

            # condor_q failed -- job is gone
            info['gone'] = True

            if not self._adaptor.use_hist:

                # look the other way and pray...
                info['returncode'] = 0
                info['state']      = api.DONE


            elif info['state'] in [api.RUNNING, api.PENDING]:

                # run the Condor 'condor_history' command to get info about
                # finished jobs
                self._logger.info("use condor_history on job %s", pid)
                ret, out, err = self.shell.run_sync("unset GREP_OPTIONS; %s -long -match 1 %s | \
                    grep -E '(ExitCode)|(CompletionDate)|(JobCurrentStartDate)|(QDate)|(Err)|(Out)'" \
                    % (self._commands['condor_history'], pid))

                if ret != 0:
                    # no job hist either - declar job as MIA
                    self._logger.error("Error using 'condor_history': %s : %s", out, err)
                    info['returncode'] = 1

                else:
                    # parse the egrep result. this should look something like this:
                    # ExitStatus = 0
                    results = out.split('\n')
                    for result in results:
                        if len(result.split('=')) == 2:
                            key, val = result.split('=')
                            key = key.strip()  # strip() removes whitespaces at the
                            val = val.strip()  # beginning and the end of the string

                            if   key == 'ExitCode'           : info['returncode']  = int(val)
                            elif key == 'QDate'              : info['create_time'] = val
                            elif key == 'JobCurrentStartDate': info['start_time']  = val
                            elif key == 'CompletionDate'     : info['end_time']    = val
                            elif key == 'Out'                : info['stdout']      = val.strip('"')
                            elif key == 'Err'                : info['stderr']      = val.strip('"')

                if info['returncode'] == 0: info['state'] = DONE
                else                      : info['state'] = FAILED

        if info['gone']:
            td = info['td']
            self._handle_file_transfers(td, mode='out')

        info['timestamp'] = time.time()


    # ----------------------------------------------------------------
    #
    def _job_get_info_bulk(self, cluster_id, job_ids):
        """ get job attributes via condor_q
        """

        # NOTE: bulk queries ignore the cache timeout,
        #       but they do update the timestamps

        # if the 'gone' flag is set, there's no need to query the job
        # state again. it's gone forever -- but we check all others
        found    = list()  # keep track of jobs for which we found new info
        to_check = list()  # keep track of jobs for which we found new info

        # we don't need to check final jobs
        for job_id in job_ids:

            if job_id not in self.jobs:
                raise ValueError('job %s: unknown')

            if self.jobs[job_id]['gone']:
                self._logger.debug('dont check %s', job_id)
                found.append(job_id)
            else:
                to_check.append(job_id)

        # do we have anything to do?
        if not to_check:
            return

        # run the Condor 'condor_q' command to get some infos about our job
        opts = "%s -autoformat:, ProcId JobStatus ExitStatus ExitBySignal CompletionDate" % cluster_id
        ret, out, err = self._run_condor_q(retries=3, timeout=60, options=opts)
        self._logger.debug('got state info:%s\n%s\n%s\n%s', opts, ret, out, err)

        if ret != 0:
            raise Exception("condor_q failed\n[%s]\n[%s]" % (out, err))

        results = list(filter(bool, out.split('\n')))
        ts      = time.time()
        for row in results:

            self._logger.debug('row: %s', row)

            # Some processes in cluster found with condor_q!
            procid, jobstatus, exit_code, exit_by_signal, completiondate \
                    = [col.strip() for col in row.split(',')]

            self._logger.debug('   : %s - %s - %s - %s - %s',
                    procid, jobstatus, exit_code, exit_by_signal, completiondate)

            # we always set exit_code to '1' if exited_by_signal
            if not exit_code and exit_by_signal == 'true':
                exit_code = 1

            matched = False
            for job_id in to_check:

                if job_id.endswith('.%s]' % procid):

                    self._logger.debug('match: %s', job_id)

                    found.append(job_id)
                    info = self.jobs[job_id]
                    info['state']      = _condor_to_saga_jobstate(jobstatus)
                    info['end_time']   = completiondate
                    info['returncode'] = exit_code
                    info['timestamp']  = ts
                    matched = True
                    self._logger.debug('     : %s', info['state'])
                    break

            if not matched:
                self._logger.warning('cannot match job info to any known job (%s)', row)


        # Now, see if any ids are missing, and search condor history for those:
        missing = [x for x in job_ids if x not in found]

        if self._adaptor.use_hist and missing:

            self._logger.debug('incomplete %s: %s', len(missing), missing)

            self._logger.info("use condor_history on cluster %s", cluster_id)
            cmd = "%s %s -autoformat:, " \
                  "ProcId ExitCode ExitBySignal CompletionDate " \
                  "JobCurrentStartDate QDate Err Out" \
                  % (self._commands['condor_history'], cluster_id)
            ret, out, err = self.shell.run_sync(cmd)

            if ret != 0:
                # we consider this non-fatal, as that sometimes failes on the
                # XSEDE OSG bridge without any further indication of errors
                # raise Exception("Error running 'condor_history' (%s) (%s)" % (out, err))
                self._logger.warning("condor_history failed: (%s) (%s)", out, err)
                out = ''

            else:
                results = list(filter(bool, out.split('\n')))
                ts      = time.time()
                for row in results:

                    self._logger.debug('hist row: %s', row)

                    elems = [col.strip() for col in row.split(',')]
                    if len(elems) != 8:
                        self._logger.error('condor_history noise [%s]', row)
                        continue

                    procid, exit_code, exit_by_signal,  \
                    cdate, sdate, qdate, stderr, stdout = elems

                    # we always set exit_code to '1' if exited_by_signal
                    if not exit_code and exit_by_signal == 'true':
                        exit_code = 1

                    # make sure exit code is an int:
                    try:
                        exit_code = int(exit_code)
                    except:
                        # no exit code looks wrong, we assume that condor
                        # failed, and thus also fail the job
                        self._logger.warning("condor_history w/o exit code - assume error")
                        exit_code = -1

                    matched = False
                    for job_id in to_check:

                        if job_id.endswith('.%s]' % procid):

                            matched = True

                            if job_id not in missing:
                                self._logger.debug('not missed: %s', job_id)
                                continue

                            self._logger.debug('match hist: %s: %s', job_id, elems)
                            found.append(job_id)
                            info = self.jobs[job_id]
                            info['returncode']  = int(exit_code)
                            info['create_time'] = qdate
                            info['start_time']  = sdate
                            info['end_time']    = cdate
                            info['stdout']      = stdout
                            info['stderr']      = stderr

                            if int(exit_code) == 0: info['state'] = DONE
                            else                  : info['state'] = FAILED

                            self._logger.debug('move state of %s to %s', job_id, info['state'])

                            info['gone']      = True
                            info['timestamp'] = ts
                            break

                    if not matched:
                        self._logger.warning('cannot match job info to any known job (%s)', row)


        # are still any jobs missing?
        missing = [x for x in job_ids if x not in found]
        if missing:

            # alas, condor_history seems not to work on the osg xsede bridge, so
            # we cannot consider this an error.  We will handle all remaining
            # jobs as disappeared, ie. as DONE.
            self._logger.warning('could not find all jobs %s: %s', len(missing), missing)

            ts = time.time()
            for job_id in missing:
                self._logger.warning('jobs %s disappeared', job_id)
                info = self.jobs[job_id]
                info['state']     = DONE
                info['gone']      = True
                info['timestamp'] = ts


        # FIXME: Transfer stuff.  Use self._handle_transfers(out)
        #
        # if info['gone'] is True:
        #     # If we are running over SSH, copy the output to our local system
        #     if self.shell.url.scheme in ["ssh", 'gsissh']:
        #         files = []
        #
        #         if info['transfers']:
        #             t = info['transfers']
        #             self._logger.debug("TransferOutput: %s", t)
        #
        #             # Remove leading and ending double quotes
        #             if t.startswith('"') and t.endswith('"'):
        #                 t = t[1:-1]
        #
        #             # Parse comma separated list
        #             files += t.split(',')
        #
        #         if info['stdout']:
        #             t = info['stdout']
        #             self._logger.debug("StdOut: %s", t)
        #
        #             # Remove leading and ending double quotes
        #             if t.startswith('"') and t.endswith('"'):
        #                 t = t[1:-1]
        #
        #             files.append(t)
        #
        #         if info['stderr']:
        #             t = info['stderr']
        #             self._logger.debug("StdErr: %s", t)
        #
        #             # Remove leading and ending double quotes
        #             if t.startswith('"') and t.endswith('"'):
        #                 t = t[1:-1]
        #
        #             files.append(t)
        #
        #         # Transfer list of files
        #         for f in files:
        #             f = f.strip()
        #             self._logger.info("Transferring file %s", f)
        #             self.shell.stage_from_remote(f, f)


    # ----------------------------------------------------------------
    #
    def _job_cancel_bulk(self, cluster_id, job_ids):
        """ cancel jobs via condor_rm
        """

        # NOTE: bulk queries ignore the cache timeout,
        #       but they do update the timestamps

        self._logger.debug('cancel cluster %s %s', cluster_id, job_ids)

        condor_rm = self._commands['condor_rm']

        for job_id in job_ids:

            # if we don't have the job in our dictionary, we don't want it
            if job_id not in self.jobs:
                raise ValueError("Unknown job ID: %s. Can't cancel." % job_id)

            info = self.jobs[job_id]

            if info['gone']:
                self._logger.warning("Job is already gone.")

        # run the Condor 'condor_q' command to get some infos about our job
        ret, out, _ = self.shell.run_sync("%s %s" % (condor_rm, cluster_id))

        if ret != 0:
            raise RuntimeError("condor_rm failed")

        for job_id in job_ids:
            self._logger.debug('canceled %s', job_id)
            info['state'] = CANCELED


    # ----------------------------------------------------------------
    #
    def _job_get_state(self, job_id):
        """ get the job's state
        """
        # check if we have already reach a terminal state
        if self.jobs[job_id]['state'] in [CANCELED, FAILED, DONE]:
            return self.jobs[job_id]['state']

        # check if we can / should update
        if self.jobs[job_id]['gone'] is not True:
            self._job_get_info(job_id=job_id)

        return self.jobs[job_id]['state']


    # ----------------------------------------------------------------
    #
    def _job_get_exit_code(self, job_id):
        """ get the job's exit code
        """
        # check if we can / should update
        if  (self.jobs[job_id]['gone'] is not True) and \
            (self.jobs[job_id]['returncode'] is None):
            self._job_get_info(job_id=job_id)

        ret = self.jobs[job_id]['returncode']

        # FIXME: 'None' should cause an exception
        if ret == None: return None
        else          : return int(ret)


    # ----------------------------------------------------------------
    #
    def _job_get_name(self, job_id):
        """ get the job's name
        """
        return self.jobs[job_id]['name']


    # ----------------------------------------------------------------
    #
    def _job_get_execution_hosts(self, job_id):
        """ get the job's exit code
        """
        # check if we can / should update
        if  (self.jobs[job_id]['gone']       is not True) and \
            (self.jobs[job_id]['exec_hosts'] is     None):
            self._job_get_info(job_id=job_id)

        return self.jobs[job_id]['exec_hosts']


    # ----------------------------------------------------------------
    #
    def _job_get_create_time(self, job_id):
        """ get the job's creation time
        """

        # check if we can / should update
        if  (self.jobs[job_id]['gone']        is not True) and \
            (self.jobs[job_id]['create_time'] is     None):
            self._job_get_info(job_id=job_id)

        return self.jobs[job_id]['create_time']


    # ----------------------------------------------------------------
    #
    def _job_get_start_time(self, job_id):
        """ get the job's start time
        """
        # check if we can / should update
        if  (self.jobs[job_id]['gone']       is not True) and \
            (self.jobs[job_id]['start_time'] is     None):
            self._job_get_info(job_id=job_id)

        return self.jobs[job_id]['start_time']


    # ----------------------------------------------------------------
    #
    def _job_get_end_time(self, job_id):
        """ get the job's end time
        """
        # check if we can / should update
        if  (self.jobs[job_id]['gone']     is not True) and \
            (self.jobs[job_id]['end_time'] is     None):
            self._job_get_info(job_id=job_id)

        # FIXME: convert to EPOCH
        return self.jobs[job_id]['end_time']


    # ----------------------------------------------------------------
    #
    def _job_cancel(self, job_id):
        """ cancel the job via 'condor_rm'
        """
        _, pid = self._adaptor.parse_id(job_id)

        ret, out, _ = self.shell.run_sync("%s %s\n"
            % (self._commands['condor_rm'], pid))

        if ret != 0:
            message = "Error canceling job via 'condor_rm': %s" % out
            log_error_and_raise(message, NoSuccess, self._logger)

        # assume the job was successfully canceled
        self.jobs[job_id]['state'] = CANCELED


    # ----------------------------------------------------------------
    #
    def _job_wait(self, job_id, timeout):
        """ wait for the job to finish or fail
        """

        time_start = time.time()
        time_now   = time_start
        rm, pid    = self._adaptor.parse_id(job_id)

        while True:
            state = self._job_get_state(job_id=job_id)

            if state in [DONE, FAILED, CANCELED]:
                    return True

            # avoid busy poll
            time.sleep(0.5)

            # check if we hit timeout
            if timeout >= 0:
                time_now = time.time()
                if time_now - time_start > timeout:
                    return False


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def create_job(self, jd):
        """ implements cpi.job.Service.get_url()
        """
        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         "job_description": jd,
                         "job_schema":      self.rm.schema,
                         "reconnect":       False
                        }

        return api.Job(_adaptor=self._adaptor, _adaptor_state=adaptor_state)



    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_job(self, jobid):
        """ Implements cpi.job.Service.get_job()
        """

        # try to get some information about this job and throw it into
        # our job dictionary.
        self.jobs[jobid] = self._retrieve_job(jobid)

        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        adaptor_state = {"job_service":     self,
                         # TODO: fill job description
                         "job_description": api.Description(),
                         "job_schema":      self.rm.schema,
                         "reconnect":       True,
                         "reconnect_jobid": jobid
                        }

        return api.Job(_adaptor=self._adaptor, _adaptor_state=adaptor_state)



    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url(self):
        """ implements cpi.job.Service.get_url()
        """
        return self.rm


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self):
        """ implements cpi.job.Service.list()
        """

        ids = []
        ret, out, err = self._run_condor_q(retries=3, timeout=60)

        # instead of listing all jobs and grep'ing for our user ID, we only list
        # jobs submitted by us.  The result is the same, but it reduces the load
        # on the condor backend.  The drawback is that it raises and error if no
        # jobs are found - we gracefully accept that and then list no jobs,
        # which is ok, but distinguishing this from other errors is somewhat of
        # a guess.  Since we *do* see other error modes pop up from time to
        # time, we at this point ignore them all.
        if ret != 0 and len(out) > 0:
            self._logger.warning("failed to list jobs via 'condor_q': [%s] [%s]",
                              out, err)

        elif ret != 0 and len(out) == 0:
            pass

        else:

            # ------------------------------------------------------------------
            # $ /usr/bin/condor_q
            #
            #
            # -- Schedd: xd-login.opensciencegrid.org : <129.79.53.198:9615?... @ 09/27/17 07:08:42
            # OWNER  BATCH_NAME       SUBMITTED   DONE   RUN    IDLE  TOTAL JOB_IDS
            # merzky CMD: /bin/env   9/27 07:07      _      1      _      1 70722204.0
            # merzky CMD: /bin/env   9/27 07:07      _      1      _      1 70722205.0
            #
            # 2 jobs; 0 completed, 0 removed, 0 idle, 2 running, 0 held, 0 suspended
            #
            # ------------------------------------------------------------------

            for line in out.split("\n"):

                line = line.strip()

                if not line              : continue
                if line.startswith('-- '): continue
                if 'OWNER'  in line      : continue
                if 'jobs; ' in line      : continue

                elems = line.split()

                if len(elems) > 1:

                    rm_clone = ru.Url(self.rm)  # TODO: this is costly
                    rm_clone.query = ""
                    rm_clone.path  = ""

                    jobid = "[%s]-[%s]" % (rm_clone, elems[0])
                    ids.append(jobid)

        return ids


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_run(self, jobs):

        # the xsede condor bridge does not accept job clusters with individual
        # project IDs, thus we cluster the given jobs by project IDs, and create
        # individual submission scripts for each, and run them.
        clusters = dict()
        for job in jobs:

            jd = job.description

            # ensure consistency and viability of job description
            self._prepare_jd(jd)

            # TODO: Given that input (and output) are likely similar for
            #       bulk tasks, we probably don't want to transfer
            #       duplicates endlessly
            self._handle_file_transfers(jd.transfer_directives, mode='in')

            project = jd.project

            if not project:
                project = ''

            if project not in clusters:
                clusters[project] = list()

            clusters[project].append(job)

        for project, _jobs in clusters.items():
            self._run_cluster(project, _jobs)


    # --------------------------------------------------------------------------
    #
    def _run_cluster(self, project, jobs):

        jds = [job.description for job in jobs]

        # create a Condor job script from SAGA job description
        script = _condorscript_generator(url=self.rm, logger=self._logger,
                                         jds=jds, option_dict=self.query_options)
        self._logger.info("Generated Condor script: %s", script)

        submit_file = NamedTemporaryFile(mode='w', suffix='.condor',
                                         prefix='tmp-radica-saga-', delete=False)
        submit_file_name = os.path.basename(submit_file.name)
        submit_file.write(script)
        submit_file.close()
        self._logger.info("Written Condor script locally: %s", submit_file.name)

        if self.shell.url.scheme in ["ssh", "gsissh"]:
            self._logger.info("Transferring Condor script to: %s", self.shell.url)
            self.shell.stage_to_remote(submit_file.name, submit_file_name)

        else:
            raise NotImplementedError("%s support for Condor not implemented." %
                    self.shell.url.scheme)

        ret, out, _ = self.shell.run_sync('%s -verbose %s'
                    % (self._commands['condor_submit'], submit_file_name))

        if ret != 0:
            # condor_submit went wrong
            message = "Error running job via 'condor_submit': %s. Script was: %s" \
                      % (out, script)
            log_error_and_raise(message, NoSuccess, self._logger)

        pids = []
        # stdout contains the job id
        for line in out.split("\n"):
            if line.startswith("** Proc "):
                pid = line.split()[2][:-1]
                if len(pid.split('.')) != 2:
                    raise Exception("Pid looks weird: %s" % pid)
                pids.append(pid)

        if len(pids) != len(jobs):
            raise Exception("Number of pids (%d) != number of jobs (%d)"
                           % (len(pids), len(jobs)))

        # we don't want the 'query' part of the URL to be part of the ID,
        # simply because it can get terribly long (and ugly). to get rid
        # of it, we clone the URL and set the query part to None.
        rm_clone       = ru.Url(self.rm)
        rm_clone.query = ""
        rm_clone.path  = ""

        for job, pid in zip(jobs, pids):
            job_id = "[%s]-[%s]" % (rm_clone, pid)

            job._adaptor._id      = job_id
            job._adaptor._started = True

            self._logger.info("Submitted Condor job with id: %s", job_id)

            # add job to internal list of known jobs.
            self.jobs[job_id] = self._new_job_info()
            self.jobs[job_id]['state'] = PENDING
            self.jobs[job_id]['td']    = job.description.transfer_directives

        # remove submit file(s)
        # XXX: maybe leave them in case of debugging?
        # ret, out, _ = self.shell.run_sync ('rm %s' % submit_file_name)

        os.remove(submit_file.name)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_wait(self, jobs, mode, timeout):

        # FIXME: implement modes
      # if mode == ANY:
      #     pass
      # if mode == ALL:
      #     pass

        while True:
            states = self.container_get_states(jobs)
            if RUNNING not in states and PENDING not in states:
                break
            time.sleep(5)

        return states[0]


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_cancel(self, jobs, timeout=None):

        # JobIds also include the rm, so we strip that out.  Then sort in
        # clusters we can query
        clusters = dict()
        for job in jobs:

            job_id     = job._adaptor._id
            proc_id    = self._adaptor.parse_id(job_id)[1]
            cluster_id = proc_id.split('.', 1)[0]

            if not cluster_id in clusters:
                clusters[cluster_id] = list()
            clusters[cluster_id].append(job_id)


        for cluster_id in clusters:

            job_ids   = clusters[cluster_id]
            bulk_info = self._job_cancel_bulk(cluster_id, job_ids)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def container_get_states(self, jobs):

        log = self._logger

        # JobIds also include the rm, so we strip that out.  Then sort in
        # clusters we can query
        clusters = dict()
        for job in jobs:

            job_id     = job._adaptor._id
            proc_id    = self._adaptor.parse_id(job_id)[1]
            cluster_id = proc_id.split('.', 1)[0]

            log.debug('get bulk state for %s %s %s', job_id, proc_id, cluster_id)

            if cluster_id not in clusters:
                clusters[cluster_id] = list()
            clusters[cluster_id].append(job_id)


        states = list()
        for cluster_id in clusters:

            job_ids   = clusters[cluster_id]
            log.debug(' query job state for %s', job_ids)
            bulk_info = self._job_get_info_bulk(cluster_id, job_ids)
            states   += [self.jobs[job_id]['state'] for job_id in job_ids]

        return states

        # TODO: check "cache" for final state jobs
        # check if we have already reach a terminal state
        # if self.jobs[job_id]['state'] in [CANCELED, FAILED, DONE]:
        #     return self.jobs[job_id]['state']
        #
        # # check if we can / should update
        # if (self.jobs[job_id]['gone'] is not True):
        #     self._job_get_info(job_id=job_id)
        #
        # return self.jobs[job_id]['state']


###############################################################################
#
class CondorJob (cpi.job.Job):
    """ implements cpi.job.Job
    """

    def __init__(self, api, adaptor):

        # initialize parent class
        _cpi_base = super(CondorJob, self)
        _cpi_base.__init__(api, adaptor)


    @SYNC_CALL
    def init_instance(self, job_info):
        """ implements cpi.job.Job.init_instance()
        """
        # init_instance is called for every new api.Job object
        # that is created
        self.jd = job_info["job_description"]
        self.js = job_info["job_service"]

        self._name = self.jd.name

        # the js is responsible for job bulk operations -- which
        # for jobs only work for run()
        self._container = self.js

        if job_info['reconnect'] is True:
            self._id      = job_info['reconnect_jobid']
            self._started = True
        else:
            self._id      = None
            self._started = False

        return self.get_api()

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_description(self):
        return self.jd


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_state(self):
        """ implements cpi.job.Job.get_state()
        """
        if self._started is False:
            # jobs that are not started are always in 'NEW' state
            return NEW
        else:
            return self.js._job_get_state(self._id)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def wait(self, timeout):
        """ implements cpi.job.Job.wait()
        """
        if self._started is False:
            log_error_and_raise("Can't wait for job that hasn't been started",
                IncorrectState, self._logger)
        else:
            self.js._job_wait(self._id, timeout)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def cancel(self, timeout):
        """ implements cpi.job.Job.cancel()
        """
        if self._started is False:
            log_error_and_raise("Can't wait for job that hasn't been started",
                IncorrectState, self._logger)
        else:
            self.js._job_cancel(self._id)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def run(self):
        """ implements cpi.job.Job.run()
        """
        self._id = self.js._job_run(self.jd)
        self._started = True


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_service_url(self):
        """ implements cpi.job.Job.get_service_url()
        """
        return self.js.rm


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_id(self):
        """ implements cpi.job.Job.get_id()
        """
        return self._id


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_name (self):
        """ Implements cpi.job.Job.get_name() """
        return self._name


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_exit_code(self):
        """ implements cpi.job.Job.get_exit_code()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_exit_code(self._id)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_created(self):
        """ implements cpi.job.Job.get_created()
        """
        if self._started is False:
            return None
        else:
            # FIXME: convert to EPOCH
            return self.js._job_get_create_time(self._id)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_started(self):
        """ implements cpi.job.Job.get_started()
        """
        if self._started is False:
            return None
        else:
            # FIXME: convert to EPOCH
            return self.js._job_get_start_time(self._id)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_finished(self):
        """ implements cpi.job.Job.get_finished()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_end_time(self._id)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_execution_hosts(self):
        """ implements cpi.job.Job.get_execution_hosts()
        """
        if self._started is False:
            return None
        else:
            return self.js._job_get_execution_hosts(self._id)


# ------------------------------------------------------------------------------

