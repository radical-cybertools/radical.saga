
""" SSH job adaptor implementation """

from saga.utils.job.jobid import JobId
from saga.utils.which     import which

import saga.adaptors.cpi.base
import saga.adaptors.cpi.job

SYNC_CALL  = saga.adaptors.cpi.base.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.base.ASYNC_CALL

# --------------------------------------------------------------------
# server side job management script
_SSH_JOB_MANAGER = """#!/bin/sh

# --------------------------------------------------------------------
# ERROR and RETVAL are used for return state from function calls
ERROR=""
RETVAL=""

# this is where this 'daemon' keeps state
BASE=$HOME/.saga/adaptors/ssh_job/

# --------------------------------------------------------------------
get_cmd () {
  if test -z $1 ; then ERROR="no command given"; return; 
  else                 RETVAL=$1;                fi
}
# --------------------------------------------------------------------
get_args () {
  if test -z $1 ; then ERROR="no command given"; return; fi 
  shift # discard command
  RETVAL=""
  while test $# -gt 0
  do
    RETVAL="$RETVAL$1 "
    shift
  done
}
# --------------------------------------------------------------------
verify_pid () {
  if test -z $1 ;            then ERROR="no pid given";              return 1; fi 
  DIR="$BASE/$1"
  if ! test -d "$DIR";       then ERROR="pid $1 not known";          return 1; fi 
  if ! test -r "$DIR/pid";   then ERROR="pid $1 in incorrect state"; return 1; fi 
  if ! test -r "$DIR/state"; then ERROR="pid $1 in incorrect state"; return 1; fi
}
# --------------------------------------------------------------------
cmd_run () {
  cmd_run_process $args &
  RETVAL=$!
}

cmd_run_process () {
  PID=`sh -c 'echo $PPID'`
  DIR="$BASE/$PID"

  mkdir -p "$DIR"   || exit 1
  echo "$args"       > "$DIR/cmd"
  echo "RUNNING"     > "$DIR/state"
  touch                "$DIR/in"
  $args \
    <  "$DIR/in"   \
    >  "$DIR/out"  \
    2> "$DIR/err"  \
    && echo "DONE"   > "$DIR/state" \
    || echo "FAILED" > "$DIR/state" &
  rpid=$!
  echo $rpid         > "$DIR/pid"
  wait $rpid
  echo $?            > "$DIR/exit"
}
# --------------------------------------------------------------------
cmd_state () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  RETVAL=`cat "$DIR/state"`
}
# --------------------------------------------------------------------
cmd_suspend () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  state=`cat "$DIR/state"`
  rpid=`cat "$DIR/pid"`

  if ! test "$state" = "RUNNING"; then
    ERROR="job $1 in incorrect state ($state != RUNNING)"
    return
  fi

  RETVAL=`kill -STOP $rpid 2>&1`
  ECODE=$?

  if ! test "$ECODE" = "0" ; then
    ERROR="suspend failed ($ECODE): $RETVAL"
  else
    RETVAL="$1 suspended"
  fi

  mv   "$DIR/state" "$DIR/state.susp"
  echo "SUSPENDED" > "$DIR/state"
}
# --------------------------------------------------------------------
cmd_resume () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  state=`cat $DIR/state`
  rpid=`cat $DIR/pid`

  if ! test "$state" = "SUSPENDED"; then
    ERROR="job $1 in incorrect state ($state != SUSPENDED)"
    return
  fi

  RETVAL=`kill -CONT $rpid 2>&1`
  ECODE=$?

  if ! test "$ECODE" = "0" ; then
    ERROR="resume failed ($ECODE): $RETVAL"
    return
  else
    RETVAL="$1 resumed"
  fi

  mv "$DIR/state.susp" "$DIR/state"
}
# --------------------------------------------------------------------
cmd_cancel () {
  verify_pid $1 || return

  DIR="$BASE/$1"

  state=`cat $DIR/state`
  rpid=`cat $DIR/pid`

  if ! test "$state" = "SUSPENDED" -o "$state" = "RUNNING"; then
    ERROR="job $1 in incorrect state ($state != SUSPENDED|RUNNING)"
    return
  fi

  RETVAL=`kill -KILL $rpid 2>&1`
  ECODE=$?

  if ! test "$ECODE" = "0" ; then
    state=`cat $DIR/state`
    if ! test "$state" = "DONE" -o "$state" = "FAILED"; then
      ERROR="job $1 in incorrect state ($state != SUSPENDED|RUNNING)"
    else
      ERROR="cancel failed ($ECODE): $RETVAL"
    fi
    return
  else
    RETVAL="$1 canceled"
  fi

  echo "CANCELED" > "$DIR/state"
}
# --------------------------------------------------------------------
cmd_stdin () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  shift
  echo "$*" >> "$DIR/in"
  RETVAL="stdin refreshed"
}
# --------------------------------------------------------------------
cmd_stdout () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  RETVAL=`uuencode "$DIR/out" "/dev/stdout"`
}
# --------------------------------------------------------------------
cmd_stderr () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  RETVAL=`uuencode "$DIR/err" "/dev/stdout"`
}
# --------------------------------------------------------------------
cmd_list () {
  for d in "$BASE"/*; do
    RETVAL="$RETVAL`basename $d` "
  done

  if test "$RETVAL" = "* "; then RETVAL=""; fi
}
# --------------------------------------------------------------------
cmd_purge () {

  if test -z "$1" ; then
    for d in `grep -l -e 'DONE' -e 'FAILED' -e 'CANCELED' "$BASE"/*/state`; do
      dir=`dirname $d`
      id=`basename $dir`
      rm -rf "$BASE/$id"
    done
    RETVAL="purged finished jobs"
    return
  fi

  verify_pid $1 || return

  DIR="$BASE/$1"
  rm -rf "$DIR"
  RETVAL="purged $1"
}
# --------------------------------------------------------------------
listen() {
  
  while read LINE; do
    ERROR="OK"
    RETVAL=""

    get_cmd  $LINE ; cmd=$RETVAL
    get_args $LINE ; args=$RETVAL

    if ! test $ERROR = "OK"; then
      echo "ERROR"; echo $ERROR
      continue
    fi

    case $cmd in
      RUN     ) cmd_run     $args ;; 
      SUSPEND ) cmd_suspend $args ;; 
      RESUME  ) cmd_resume  $args ;; 
      CANCEL  ) cmd_cancel  $args ;; 
      STATE   ) cmd_state   $args ;; 
      STDIN   ) cmd_stdin   $args ;; 
      STDOUT  ) cmd_stdout  $args ;; 
      STDERR  ) cmd_stderr  $args ;; 
      LIST    ) cmd_list    $args ;; 
      PURGE   ) cmd_purge   $args ;; 
      QUIT    ) echo "OK"; exit 0 ;;
      *       ) ERROR="$cmd unknown ($LINE)" ;; 
    esac

    if ! test "$ERROR" = "OK"; then
      echo "ERROR"; echo $ERROR
    else
      echo "OK";    echo "$RETVAL"
    fi
  done
}
# --------------------------------------------------------------------
listen
# --------------------------------------------------------------------

"""



# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = 'saga.adaptor.ssh_job'
_ADAPTOR_SCHEMAS       = ['ssh', 'gsissh']
_ADAPTOR_OPTIONS       = []

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES  = {
    'jdes_attributes'  : [saga.job.EXECUTABLE,
                          saga.job.ARGUMENTS,
                          saga.job.ENVIRONMENT,
                          saga.job.INPUT,
                          saga.job.OUTPUT,
                          saga.job.ERROR],
    'job_attributes'   : [saga.job.EXIT_CODE,
                          saga.job.EXECUTION_HOSTS,
                          saga.job.CREATED,
                          saga.job.STARTED,
                          saga.job.FINISHED],
    'metrics'          : [saga.job.STATE, 
                          saga.job.STATE_DETAIL],
    'contexts'         : {'ssh'  : "an public/private keypair",
                          'x509' : "a X509 proxy for gsissh" }
    }
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilities'     : _ADAPTOR_CAPABILITIES,
    'description'      : """ 
        The SSH job adaptor. This adaptor uses the ssh command line tools to run
        remote jobs.
        """,
    'details'          : """ 
        A more elaborate description....
        """,
    'schemas'          : {'ssh'    :'use ssh to run a remote job', 
                          'gsissh' :'use gsissh to run a remote job'}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.1',
    'schemas'          : _ADAPTOR_SCHEMAS,
    'cpis'             : [
        { 
        'type'         : 'saga.job.Service',
        'class'        : 'SSHJobService'
        }, 
        { 
        'type'         : 'saga.job.Job',
        'class'        : 'SSHJob'
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


    def __init__ (self) :

        saga.adaptors.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        # we only need to call gethostname once 
        self.hostname = socket.gethostname()


    def sanity_check (self) :
        pass



###############################################################################
#
class SSHJobService (saga.adaptors.cpi.job.Service) :
    """ Implements saga.adaptors.cpi.job.Service
    """
    def __init__ (self, api, adaptor) :
        """ Implements saga.adaptors.cpi.job.Service.__init__
        """
        saga.adaptors.cpi.CPIBase.__init__ (self, api, adaptor)


    @SYNC_CALL
    def init_instance (self, adaptor_state, rm_url, session) :
        """ Service instance constructor
        """

        print " state  : %s " % str(adaptor_state)
        print " rm_url : %s " % str(rm_url)
        print " session: %s " % str(session)

        # check that the hostname is supported
        fqhn = Adaptor().hostname
        if rm_url.host != 'localhost' and rm_url.host != fqhn:
            msg = "Only 'localhost' and '%s' hostnames supported by this adaptor'" % (fqhn)
            raise saga.BadParameter._log (self._logger, msg)

        self._rm      = rm_url
        self._session = session

        # holds the jobs that were started via this instance
        self._jobs = dict() # {job_obj:id, ...}

        return self._api


    def _register_job(self, job_obj):
        """ Register job and job id with this service instance.  

            This is a convenience method and not part of the CPI.  A job can be
            registered repeatedly, in particular for delayed assignment of job
            IDs.
        """
        self._jobs[job_obj] = job_obj._id


    @SYNC_CALL
    def get_url (self) :
        """ Implements saga.adaptors.cpi.job.Service.get_url()
        """
        return self._rm

    @SYNC_CALL
    def list(self):
        """ Implements saga.adaptors.cpi.job.Service.list()
        """
        jobids = list()
        for (job_obj, job_id) in self._jobs.iteritems():
            if job_id is not None:
                jobids.append(job_id)
        return jobids

    @SYNC_CALL
    def create_job (self, jd) :
        """ Implements saga.adaptors.cpi.job.Service.get_url()
        """
        # check that only supported attributes are provided
        for attribute in jd.list_attributes():
            if attribute not in _ADAPTOR_CAPABILITIES['desc_attributes']:
                msg = "'JobDescription.%s' is not supported by this adaptor" % attribute
                raise saga.BadParameter._log (self._logger, msg)

        
        # this dict is passed on to the job adaptor class -- use it to pass any
        # state information you need there.
        state = { 'job_service'     : self, 
                  'job_description' : jd, 
                  'session'         : self._session }

        return saga.job.Job (_adaptor=self._adaptor, _adaptor_state=state)


    @SYNC_CALL
    def get_job (self, jobid):
        """ Implements saga.adaptors.cpi.job.Service.get_url()
        """
        if jobid not in self._jobs.values():
            msg = "Service instance doesn't know a Job with ID '%s'" % (jobid)
            raise saga.BadParameter._log (self._logger, msg)
        else:
            for (job_obj, job_id) in self._jobs.iteritems():
                if job_id == jobid:
                    return job_obj._api


    def container_run (self, jobs) :
        self._logger.debug("container run: %s"  %  str(jobs))
        # TODO: this is not optimized yet
        for job in jobs:
            job.run()


    def container_wait (self, jobs, mode, timeout) :
        self._logger.debug("container wait: %s"  %  str(jobs))
        # TODO: this is not optimized yet
        for job in jobs:
            job.wait()


    #def container_cancel (self, jobs) :
    #    self._logger.debug("container cancel: %s"  %  str(jobs))
    #    raise saga.NoSuccess("Not Implemented");


###############################################################################
#
class SSHJob (saga.adaptors.cpi.job.Job) :
    """ Implements saga.adaptors.cpi.job.Job
    """
    def __init__ (self, api, adaptor) :
        """ Implements saga.adaptors.cpi.job.Job.__init__()
        """
        saga.adaptors.cpi.CPIBase.__init__ (self, api, adaptor)

    @SYNC_CALL
    def init_instance (self, job_info):
        """ Implements saga.adaptors.cpi.job.Job.init_instance()
        """
        self._session         = job_info['session']
        self._jd              = job_info['job_description']
        self._parent_service  = job_info['job_service'] 

        # the _parent_service is responsible for job bulk operations -- which
        # for jobs only work for run()
        self._container       = self._parent_service
        self._method_type     = 'run'

        # initialize job attribute values
        self._id              = None
        self._state           = saga.job.NEW
        self._exit_code       = None
        self._exception       = None
        self._started         = None
        self._finished        = None
        self._execution_hosts = [Adaptor().hostname]
        
        # subprocess handle
        self._process         = None

        # register ourselves with the parent service
        # our job id is still None at this point
        self._parent_service._register_job(self)

        return self._api


    @SYNC_CALL
    def get_state(self):
        """ Implements saga.adaptors.cpi.job.Job.get_state()
        """
        if self._state == saga.job.RUNNING:
            # only update if still running 
            self._exit_code = self._process.poll() 
            if self._exit_code is not None:
                if self._exit_code != 0:
                    self._exception = saga.NoSuccess ("non-zero exit code : %d" % self._exit_code)
                    self._state     = saga.job.FAILED
                else:
                    self._state = saga.job.DONE
                # TODO: this is not accurate -- job could have terminated 
                # before 'wait' was called
                self._finished = time.time() 
        return self._state

    @SYNC_CALL
    def wait(self, timeout):
        if self._process is None:
            msg = "Can't wait for job that has not been started"
            raise saga.IncorrectState._log (self._logger, msg)
        if timeout == -1:
            self._exit_code = self._process.wait()
        else:
            t_beginning = time.time()
            seconds_passed = 0
            while True:
                self._exit_code = self._process.poll()
                if self._exit_code is not None:
                    break
                seconds_passed = time.time() - t_beginning
                if timeout and seconds_passed > timeout:
                    break
                time.sleep(0.5)

    @SYNC_CALL
    def get_id (self) :
        """ Implements saga.adaptors.cpi.job.Job.get_id()
        """        
        return self._id

    @SYNC_CALL
    def get_exit_code(self) :
        """ Implements saga.adaptors.cpi.job.Job.get_exit_code()
        """        
        return self._exit_code

    @SYNC_CALL
    def get_created(self) :
        """ Implements saga.adaptors.cpi.job.Job.get_started()
        """     
        # for local jobs started == created. for other adaptors 
        # this is not necessarily true   
        return self._started

    @SYNC_CALL
    def get_started(self) :
        """ Implements saga.adaptors.cpi.job.Job.get_started()
        """        
        return self._started

    @SYNC_CALL
    def get_finished(self) :
        """ Implements saga.adaptors.cpi.job.Job.get_finished()
        """        
        return self._finished
    
    @SYNC_CALL
    def get_execution_hosts(self) :
        """ Implements saga.adaptors.cpi.job.Job.get_execution_hosts()
        """        
        return self._execution_hosts

    @SYNC_CALL
    def cancel(self, timeout):
        try:
            os.killpg(self._process.pid, signal.SIGTERM)
            self._exit_code = self._process.wait() # should return with the rc
            self._state = saga.job.CANCELED
        except OSError, ex:
            msg = "Couldn't cancel job %s: %s" % (self._id, ex)
            raise saga.IncorrectState._log (self._logger, msg)


    @SYNC_CALL
    def run(self): 
        """ Implements saga.adaptors.cpi.job.Job.run()
        """

        # lots of attribute checking and such 
        executable  = self._jd.executable
        arguments   = self._jd.arguments
        environment = self._jd.environment
        cwd         = self._jd.working_directory
        
        # check if we want to write stdout to a file
        if self._jd.output is not None:
            if os.path.isabs(self._jd.output):
                self._job_output = open(jd.output,"w")  
            else:
                if cwd is not None:
                    self._job_output = open(os.path.join(cwd, self._jd.output),"w")
                else:
                    self._job_output = open(self._jd.output,"w")  
        else:
            self._job_output = None 

        # check if we want to write stderr to a file
        if self._jd.error is not None:
            if os.path.isabs(self._jd.error):
                self._job_error = open(self._jd.error,"w")  
            else:
                if cwd is not None:
                    self._job_error = open(os.path.join(self.cwd, self._jd.error),"w")
                else:
                    self._job_error = open(self._jd.error,"w") 
        else:
            self._job_error = None

        # check if we want to execute via mpirun
        if self._jd.spmd_variation is not None:
            if jd.spmd_variation.lower() == "mpi":
                if self._jd.number_of_processes is not None:
                    self.number_of_processes = self._jd.number_of_processes
                    use_mpirun = True
                self._logger.info("SPMDVariation=%s requested. Job will execute via 'mpirun -np %d'." % (self._jd.spmd_variation, self.number_of_processes))
            else:
                self._logger.warning("SPMDVariation=%s: unsupported SPMD variation. Ignoring." % self._jd.spmd_variation)
        else:
            use_mpirun = False

        # check if executable exists.
        if which(executable) == None:
            message = "Executable '%s' doesn't exist or is not in the path" % executable
            self._logger.error(message)        
            raise saga.BadParameter(message)

        # check if you can do mpirun
        if use_mpirun is True:
            which('mpirun')
            if mpirun == None:
                message = "SPMDVariation=MPI set, but can't find 'mpirun' executable."
                self._logger.error(message)        
                raise saga.BadParameter(message) 
            else:
                cmdline = '%s -np %d %s' % (mpirun, self.number_of_processes, str(self.executable))
        else:
            cmdline = str(executable)
        args = ""
        if arguments is not None:
            for arg in arguments:
                cmdline += " %s" % arg 

        # now we can finally try to run the job via subprocess
        try:
            self._process = subprocess.Popen(cmdline, shell=True, 
                                             stderr=self._job_error, 
                                             stdout=self._job_output, 
                                             env=environment,
                                             cwd=cwd,
                                             preexec_fn=os.setsid)
            self._pid = self._process.pid

            jid = JobId()
            jid.native_id   = self._pid
            jid.backend_url = str(self._parent_service.get_url())

            self._state     = saga.job.RUNNING
            self._started   = time.time()
            self._id        = str(jid)

            self._parent_service._register_job(self)
            self._logger.debug("Starting process '%s' was successful." % cmdline) 

        except Exception, ex:
            msg = "Starting process: '%s' failed: %s" % (cmdline, str(ex))
            self._exception = saga.NoSuccess._log (self._logger, msg)
            self._state     = saga.job.FAILED
            raise self._exception


    @SYNC_CALL
    def re_raise(self):
        return self._exception


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

