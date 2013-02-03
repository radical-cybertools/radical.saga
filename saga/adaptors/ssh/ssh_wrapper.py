
# --------------------------------------------------------------------
# server side job management script
_WRAPPER_SCRIPT = '''#!/bin/sh

# this script uses only POSIX shell functionality, and does not rely on bash or
# other shell extensions.  It expects /bin/sh to be a POSIX compliant shell
# thought.


# --------------------------------------------------------------------
#
# ERROR and RETVAL are used for return state from function calls
#
ERROR=""
RETVAL=""

# this is where this 'daemon' keeps state for all started jobs
BASE=$HOME/.saga/adaptors/ssh_job/

# this process will terminate when idle for longer than TIMEOUT seconds
TIMEOUT=30



# --------------------------------------------------------------------
#
# idle_checker is running in the background, and will terminate the wrapper
# shell if it is idle for longer than TIMEOUT seconds
#
trap idle_handler ALRM

idle_handler (){
  echo "IDLE TIMEOUT"
  rm -f "$BASE/idle.$ppid"
  touch "$BASE/timed_out.$ppid"
  exit 0
}

idle_checker () {

  ppid=$1

  while true
  do
    sleep $TIMEOUT

    if test -e "$BASE/idle.$ppid"
    then
      kill -s ALRM $ppid >/dev/null 2>&1
      exit 0
    fi

    touch   "$BASE/idle.$ppid"
  done
}


# --------------------------------------------------------------------
#
# utility call which extracts the first argument and returns it.
#
get_cmd () {
  if test -z $1 ; then RETVAL="NOOP"; return; 
  else                 RETVAL=$1;     fi
}


# --------------------------------------------------------------------
#
# utility call which strips the first of a set of arguments, and returns the
# remaining ones in a space separated string
#
get_args () {
  if test -z $1 ; then        RETVAL="";  return; 
  else                 shift; RETVAL=$@;  fi 
}


# --------------------------------------------------------------------
#
# utility call which ensures that a given job id points to a viable working
# directory
#
verify_pid () {
  if test -z $1 ;            then ERROR="no pid given";              return 1; fi 
  DIR="$BASE/$1"
  if ! test -d "$DIR";       then ERROR="pid $1 not known";          return 1; fi 
  if ! test -r "$DIR/pid";   then ERROR="pid $1 in incorrect state"; return 1; fi 
  if ! test -r "$DIR/state"; then ERROR="pid $1 In incorrect state"; return 1; fi
}


# --------------------------------------------------------------------
#
# run a job in the background.  Note that the returned job ID is actually the
# pid of the shell process which wraps the actual job, monitors its state, and
# serves its I/O channels.  The actual job id is stored in the 'pid' file in the
# jobs working directory.
#
# Note that the actual job is not run directly, but via nohup.  Otherwise all
# jobs would be canceled as soon as this master script finishes...
#
# Note further that we perform a double fork, effectively turning the monitor
# into a daemon.  That provides a speedup of ~300%, as the wait in cmd_run now
# will return very quickly (it just waits on the second fork).  We can achieve
# near same performance be removing the wait, but that will result in one zombie
# per command, which sticks around as long as the wrapper script itself lives.
#
# Note that the working directory is created on the fly.  As the name of the dir
# is the pid, it must be unique -- we thus purge whatever trace we find of an
# earlier directory of the same name.
#
#
# Known limitations:
#
# The script has a small race condition, between starting the job (the 'nohup'
# line), and the next line where the jobs now known pid is stored away.  I don't
# see an option to make those two ops atomic, or resilient against script
# failure - so, in worst case, you might get a running job for which the job id
# is not stored (i.e. not known).  
#
# Also, the line after is when the job state is set to 'Running' -- we can't
# really do that before, but on failure, in the worst case, we might have a job
# with known job ID which is not marked as running.  
#
# Bottom line: full disk will screw with state consistency -- which is no
# surprise really...

cmd_run () {
  #
  # do a double fork to avoid zombies (need to do a wait in this process)
  #
  # FIXME: do some checks here, such as if executable exists etc.
  # FIXME: do some checks here, such as if executable exists etc.
  # 
  # FIXME: Now, this is *the* *strangest* *error* I *ever* saw... - the above
  # two comment lines are, in source, identical -- but for local bash
  # connections, when written to disk via cat, the second line will have only 14
  # characters!  I see the correct data given to os.write(), and that is the
  # *only* place data are missing - but why?  It seems to be an offset problem:
  # removing a character earlier in this string will extend the shortened line
  # by one character.  Sooo, basically this long comment here will (a) document
  # the problem, and (b) shield the important code below from truncation.
  #
  # go figure...

  cmd_run2 $@ &
  pid=$!      # this is the (native) job id!
  wait $pid   # this will return very quickly -- look at cmd_run2... ;-)
  sync
  RETVAL=$pid # report id
}


cmd_run2 () {
  # this is the second part of the double fork -- run the actual workload in the
  # background and return - voila!  Note, no wait here, as the spawned script is
  # supposed to stay alive with the job.
  SAGA_PID=`sh -c 'echo $PPID'`
  DIR="$BASE/$SAGA_PID"

  test -d "$DIR"    && rm    -rf "$DIR"  # re-use old pid's
  test -d "$DIR"    || mkdir -p  "$DIR"  || exit 1
  echo "NEW"         > "$DIR/state"

  cmd_run_process $@ &
  ppid=$!
  return $ppid
}


cmd_run_process () {
  # this command runs the job.  PPID will point to the id of the spawning
  # script, which, coincidentally, we designated as job ID -- nice:
  PID=$SAGA_PID
  DIR="$BASE/$PID"

  echo "$@"          > "$DIR/cmd"
  touch                "$DIR/in"

  # create a script which represents the job.  The 'exec' call will replace the
  # script's shell instance with the job executable, leaving the I/O
  # redirections intact.
  cat                > "$DIR/job.sh" <<EOT
  exec $@            < "$DIR/in" >  "$DIR/out" 2> "$DIR/err"
EOT
  
  # the job script above is started by this startup script, which makes sure
  # that the job state is properly watched and captured.
  cat                > "$DIR/monitor.sh" <<EOT
    DIR=$DIR
    nohup /bin/sh      "\$DIR/job.sh" 1>/dev/null 2>/dev/null 3</dev/null &
    rpid=\$!
    echo \$rpid      > "\$DIR/pid"
    echo "RUNNING"   > "\$DIR/state"

    while true
    do
      wait \$rpid
      retv=\$?

      # if wait failed for other reason than job finishing, i.e. due to
      # suspend/resume, then we need to wait again, otherwise we are done
      # waiting...
      if test -e "\$DIR/suspended"
      then
        rm -f "\$DIR/suspended"
        # need to wait again
        continue
      fi
      if test -e "\$DIR/resumed"
      then
        rm -f "\$DIR/resumed"
        # need to wait again
        continue
      fi
      
      # real exit -- evaluate exit val
      echo \$retv > "\$DIR/exit"
      test \$retv = 0           && echo DONE      > "\$DIR/state"
      test \$retv = 0           || echo FAILED    > "\$DIR/state"

      # capture canceled state
      test -e "\$DIR/canceled"  && echo CANCELED  > "\$DIR/state"
      test -e "\$DIR/canceled"  && rm -f            "\$DIR/canceled"

      # done waiting
      break
    done

EOT

  # the monitor script is ran asynchronously and with nohup, so that its
  # lifetime will not be bound to the manager script lifetime.
  nohup /bin/sh "$DIR/monitor.sh" 1>/dev/null 2>/dev/null 3</dev/null &
  exit
}


# --------------------------------------------------------------------
#
# inspect job state
#
cmd_state () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  RETVAL=`cat "$DIR/state"`
}


# --------------------------------------------------------------------
#
# suspend a running job
#
cmd_suspend () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  state=`cat "$DIR/state"`
  rpid=`cat "$DIR/pid"`

  if ! test "$state" = "RUNNING"
  then
    ERROR="job $1 in incorrect state ($state != RUNNING)"
    return
  fi

  touch "$DIR/suspended"
  RETVAL=`kill -STOP $rpid 2>&1`
  ECODE=$?

  if test "$ECODE" = "0" 
  then
    mv    "$DIR/state" "$DIR/state.susp"
    echo SUSPENDED >   "$DIR/state"
    RETVAL="$1 suspended"
  else
    ERROR="suspend failed ($ECODE): $RETVAL"
  fi

}


# --------------------------------------------------------------------
#
# resume a suspended job
#
cmd_resume () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  state=`cat $DIR/state`
  rpid=`cat $DIR/pid`

  if ! test "$state" = "SUSPENDED"
  then
    ERROR="job $1 in incorrect state ($state != SUSPENDED)"
    return
  fi

  RETVAL=`kill -CONT $rpid 2>&1`
  ECODE=$?

  if test "$ECODE" = "0" 
  then
    mv    "$DIR/state.susp" "$DIR/state"
    touch "$DIR/resumed"
    RETVAL="$1 resumed"
  else
    ERROR="resume failed ($ECODE): $RETVAL"
  fi

}


# --------------------------------------------------------------------
#
# kill a job, and set state to canceled
#
cmd_cancel () {
  verify_pid $1 || return

  DIR="$BASE/$1"

  state=`cat $DIR/state`
  rpid=`cat $DIR/pid`

  if test "$state" != "SUSPENDED" -a "$state" != "RUNNING"
  then
    ERROR="job $1 in incorrect state ('$state' != 'SUSPENDED|RUNNING')"
    return
  fi

  touch "$DIR/canceled"
  RETVAL=`kill -KILL $rpid 2>&1`
  ECODE=$?

  if test "$ECODE" = "0" 
  then
    RETVAL="$1 canceled"
  else
    # kill failed!
    rm -f "$DIR/canceled"
    ERROR="cancel failed ($ECODE): $RETVAL"
  fi
}


# --------------------------------------------------------------------
#
# feed given string to job's stdin stream
# 
cmd_stdin () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  shift
  echo "$*" >> "$DIR/in"
  RETVAL="stdin refreshed"
}


# --------------------------------------------------------------------
#
# print uuencoded string of job's stdout
#
cmd_stdout () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  RETVAL=`uuencode "$DIR/out" "/dev/stdout"`
}


# --------------------------------------------------------------------
#
# print uuencoded string of job's stderr
#
cmd_stderr () {
  verify_pid $1 || return

  DIR="$BASE/$1"
  RETVAL=`uuencode "$DIR/err" "/dev/stdout"`
}


# --------------------------------------------------------------------
#
# list all job IDs
#
cmd_list () {
  for d in "$BASE"/*
  do
    RETVAL="$RETVAL`basename $d` "
  done

  if test "$RETVAL" = "* "; then RETVAL=""; fi
}


# --------------------------------------------------------------------
#
# purge working directories of given jobs (all non-final jobs as default)
#
cmd_purge () {

  if test -z "$1" 
  then
    for d in `grep -l -e 'DONE' -e 'FAILED' -e 'CANCELED' "$BASE"/*/state`
    do
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
#
# quit this script gracefully
#
cmd_quit () {

  # kill idle checker
  kill $1 >/dev/null 2>&1
  rm -f "$BASE/idle.$$"

  exit 0
}


# --------------------------------------------------------------------
#
# main even loop -- wait for incoming command lines, and react on them
#
listen() {
  
  # we need our home base...
  test -d "$BASE" || mkdir -p  "$BASE"  || exit 1

  # make sure we get killed when idle
  idle_checker $$ >/dev/null 2>&1 &
  idle=$!

  # prompt for commands...
  echo "PROMPT-0->"

  # and read those from stdin
  while read LINE
  do

    # reset err state for each command
    ERROR="OK"
    RETVAL=""

    get_cmd  $LINE ; cmd=$RETVAL
    get_args $LINE ; args=$RETVAL

    # did we find command and args?  Note that args may be empty, e.g. for QUIT
    if ! test "$ERROR" = "OK"
    then
      echo "ERROR"
      echo "$ERROR"
      continue
    fi

    # simply invoke the right function for each command, or complain if command
    # is not known
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
      QUIT    ) cmd_quit    $idle ;; 
      LOG     ) echo LOG    $args ;; 
      NOOP    )                   ;;
      *       ) ERROR="$cmd unknown ($LINE)"; false ;; 
    esac

    EXITVAL=$?

    # the called function will report state and results in 'ERROR' and 'RETVAL'
    if ! test "$ERROR" = "OK"; then
      echo "ERROR"     
      echo "$ERROR"
    else
      echo "OK"        
      echo "$RETVAL"
    fi

    # we did hard work - make sure we are not getting killed for idleness!
    rm -f "$BASE/idle.$$"

    # well done - prompt for next command
    echo "PROMPT-$EXITVAL->"

  done
}


# --------------------------------------------------------------------
#
# run the main loop -- that will live forever, until a 'QUIT' command is
# encountered.
#
listen
#
# --------------------------------------------------------------------

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2

'''

