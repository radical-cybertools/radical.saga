#!/bin/bash

# this script uses only POSIX shell functionality, and does not rely on bash or
# other shell extensions.  It expects /bin/sh to be a POSIX compliant shell
# thought.

# --------------------------------------------------------------------
#
# Fucking /bin/kill by Ubuntu sometimes understands --, sometimes does not :-P
# We need to check the version, and assume that prior to 3.3.0 it is not
# understood
KILL_DASHES="--"
KILL_VERSION=`/bin/kill --version 2>&1 | tr -d -c '[:digit:]'`
if test 330 -gt "$KILL_VERSION"
then
  KILL_DASHES=""
fi

# we always start in the user's home dir
\cd $HOME 2>&1 > /dev/null


# --------------------------------------------------------------------
#
# ERROR and RETVAL are used for return state from function calls
#
ERROR=""
RETVAL=""

# this is where this 'daemon' keeps state for all started jobs
BASE=$HOME/.saga/adaptors/shell_job/

# this process will terminate when idle for longer than TIMEOUT seconds
TIMEOUT=30

# update timestamp function
TIMESTAMP=0

PURGE_ON_START="%(PURGE_ON_START)s"

# default exit value is 1, for error.  We need to set explicitly to 0 for
# non-error conditions.
EXIT_VAL=1

# --------------------------------------------------------------------
#
# idle_checker is running in the background, and will terminate the wrapper
# shell if it is idle for longer than TIMEOUT seconds
#
\trap cleanup_handler QUIT TERM EXIT
# trap idle_handler ALRM
\trap '' ALRM

cleanup_handler (){
  cmd_quit $IDLE
}

idle_handler (){
  cmd_quit TIMEOUT
}

idle_checker () {

  ppid=$1

  while true
  do
    \sleep $TIMEOUT

    if test -e "$BASE/quit.$ppid" 
    then
      \rm   -f  "$BASE/quit.$ppid" 
      EXIT_VAL=0
      exit 0
    fi

    if test -e "$BASE/idle.$ppid"
    then
      /bin/kill -s ALRM $ppid >/dev/null 2>&1
      \rm   -f  "$BASE/idle.$ppid" 
      exit 0
    fi

    \touch "$BASE/idle.$ppid"
  done
}


# --------------------------------------------------------------------
#
# it is suprisingly difficult to get seconds since epoch in POSIX -- 
# 'date +%%s' is a GNU extension...  Anyway, awk to the rescue! 
#
timestamp () {
  TIMESTAMP=`\awk 'BEGIN{srand(); print srand()}'`
}


# --------------------------------------------------------------------
# ensure that a given job id points to a viable working directory
verify_dir () {
  if test -z $1 ;            then ERROR="no pid given";        return 1; fi
  DIR="$BASE/$1"
  if ! test -d "$DIR";       then ERROR="pid $1 not known";    return 1; fi
}


# --------------------------------------------------------------------
# ensure that given job id has valid pid file
verify_pid () {
  verify_dir $1
  if ! test -r "$DIR/rpid";  then ERROR="pid $1 has no process id"; return 1; fi
}


# --------------------------------------------------------------------
# ensure that given job id has valid state file
verify_state () {
  verify_dir $1
  if ! test -r "$DIR/state"; then ERROR="pid $1 has no state"; return 1; fi
}


# --------------------------------------------------------------------
# ensure that given job id has valid stdin file
verify_in () {
  verify_dir $1
  if ! test -r "$DIR/in";    then ERROR="pid $1 has no stdin"; return 1; fi
}


# --------------------------------------------------------------------
# ensure that given job id has valid stdou file
verify_out () {
  verify_dir $1
  if ! test -r "$DIR/out";   then ERROR="pid $1 has no stdout"; return 1; fi
}


# --------------------------------------------------------------------
# ensure that given job id has valid stderr file
verify_err () {
  verify_dir $1
  if ! test -r "$DIR/err";   then ERROR="stderr $1 has no sterr"; return 1; fi
}


# --------------------------------------------------------------------
#
# create the monitor script, used by the command running routines.
#
create_monitor () {
  \cat > "$BASE/monitor.sh" <<EOT

  # create the monitor wrapper script once -- this is used by all job startup
  # scripts to actually run job.sh.  The script gets SAGA_PID as argument,
  # denoting the job to monitor.   The monitor will write 3 pids to a named pipe
  # (listened to by the wrapper):
  #
  #   rpid: pid of shell running the job 
  #   mpid: pid of this monitor.sh instance (== pid of process group for cancel)
  SAGA_PID=\$1
  shift
  DIR="\$*"

  # subscript which represents the job.  The 'exec' call will replace the
  # script's shell instance with the job executable, leaving the I/O
  # redirections intact.
  \\touch "\$DIR/in"

  (
    \\printf  "RUNNING \\n"     >> "\$DIR/state"  ;
    \\exec /bin/sh "\$DIR/cmd"   < "\$DIR/in" > "\$DIR/out" 2> "\$DIR/err"
  ) 1> /dev/null 2>/dev/null 3</dev/null &

  RPID=\$!
  MPID=\$\$

  \\printf "\$RPID\\n" >  "\$DIR/rpid"  # real job id
  \\printf "\$MPID\\n" >  "\$DIR/mpid"  # monitor pid
  

  # we don't care when the wrapper sees these, print can hang forever as far as
  # we care...
  ( \\printf "OK\\n" > "\$DIR/fifo" & )
  

  while true
  do
    \\wait \$RPID
    retv=\$?

    # if wait failed for other reason than job finishing, i.e. due to
    # suspend/resume, then we need to wait again, otherwise we are done
    # waiting...
    if test -e "\$DIR/suspended"
    then
      \\rm -f "\$DIR/suspended"
      # need to wait again
      continue
    fi

    if test -e "\$DIR/resumed"
    then
      \\rm -f "\$DIR/resumed"
      # need to wait again
      continue
    fi

    STOP=\`\\awk 'BEGIN{srand(); print srand()}'\`
    \\printf "STOP  : \$STOP\\n"  >> "\$DIR/stats"

    # evaluate exit val
    \\printf "\$retv\\n" > "\$DIR/exit"

    test   "\$retv" -eq 0  && \\printf "DONE \\n"   >> "\$DIR/state"
    test   "\$retv" -eq 0  || \\printf "FAILED \\n" >> "\$DIR/state"

    # done waiting
    break
  done

  exit

EOT

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
# into a daemon.  That provides a speedup of ~300 percent, as the wait in 
# cmd_run now will return very quickly (it just waits on the second fork).  
# We can achieve near same performance be removing the wait, but that will 
# result in one zombie per command, which sticks around as long as the wrapper 
# script itself lives.
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
# see an option to make those two ops atomic, or resilient against system
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


  cmd_run2 "$@" &

  SAGA_PID=$!      # this is the (SAGA-level) job id!
  \wait $SAGA_PID  # this will return very quickly -- look at cmd_run2... ;-)

  if test "$SAGA_PID" = '0'  
  then
    # some error occured, assume RETVAL is set
    ERROR="NOK"
    return
  fi

  # success
  RETVAL=$SAGA_PID 

  # we have to wait though 'til the job enters RUNNING (this is a sync job
  # startup)
  DIR="$BASE/$SAGA_PID"

  while true
  do
    \grep "RUNNING" "$DIR/state" && break
    \sleep 0  # sleep 0 will wait for just some millisecs
  done
}


cmd_run2 () {
  # this is the second part of the double fork -- run the actual workload in the
  # background and return - voila!  Note: no wait here, as the spawned script is
  # supposed to stay alive with the job.
  #
  # NOTE: we could, in principle, separate SUBMIT from RUN -- in this case, move
  # the job into NEW state.

  # turn off debug tracing -- stdout interleaving will mess with parsing.
  set +x 

  SAGA_PID=`sh -c '\printf "$PPID"'`
  DIR="$BASE/$SAGA_PID"

  timestamp
  START=$TIMESTAMP

  test -d "$DIR"            && \rm    -rf "$DIR"     # re-use old pid if needed
  test -d "$DIR"            || \mkdir -p  "$DIR"  || (RETVAL="cannot use job id"; return 0)
  \printf "START : $START\n"  > "$DIR/stats"
  \printf "NEW \n"           >> "$DIR/state"

  cmd_run_process "$SAGA_PID" "$@" &
  DAEMON_PID=$!      # this is the (SAGA-level) job id!
  \wait $DAEMON_PID   # this will return very quickly -- look at cmd_run2... ;-)
  return $!
}


cmd_run_process () {
  # this command runs the job.  PPID will point to the id of the spawning
  # script, which, coincidentally, we designated as job ID -- nice:
  SAGA_PID=$1
  shift

  DIR="$BASE/$SAGA_PID"

  \mkfifo "$DIR/fifo"           # to communicate with the monitor
  \printf "$*\n" >  "$DIR/cmd"  # job to run by the monitor

  # start the monitor script, which makes sure
  # that the job state is properly watched and captured.
  # The monitor script is ran asynchronously, so that its
  # lifetime will not be bound to the manager script lifetime.  Also, it runs in
  # an interactive shell, i.e. in a new process group, so that we can signal the
  # monitor and the actual job processes all at once (think suspend, cancel).
  ( /bin/sh -i -c "sh $BASE/monitor.sh  $SAGA_PID \"$DIR\" 2>&1 > \"$DIR/monitor.log\" & exit" )

  \read -r TEST < "$DIR/fifo"
  \rm -rf $DIR/fifo

  exit
}


cmd_lrun () {
  # LRUN allows to run shell commands which span more than one line.
  CMD=""
  # need IFS to preserve white space,
  OLDIFS=$IFS
  IFS=
  while \read -r IN
  do
    if test "$IN" = "LRUN_EOT "
    then
      break
    fi
    CMD="$CMD$IN\n"
  done
  IFS=$OLDIFS
  cmd_run "$CMD"
}

# --------------------------------------------------------------------
#
# inspect job state
#
cmd_state () {
  verify_state $1 || return

  DIR="$BASE/$1"
  RETVAL=`\grep -e ' $' "$DIR/state" | \tail -n 1 | \tr -d ' '`
  if test "$RETVAL" = ""
  then
    RETVAL=UNKNOWN
  fi
}


# --------------------------------------------------------------------
#
# retrieve job stats
#
cmd_stats () {
  # stats are only defined for jobs in some state
  verify_state $1 || return

  DIR="$BASE/$1"
  STATE=`\grep -e ' $' "$DIR/state" | \tail -n 1 | \tr -d ' '`
  RETVAL="STATE : $STATE\n"
  RETVAL="$RETVAL\n`\cat $DIR/stats`\n"
}


# --------------------------------------------------------------------
#
# wait for job to finish.  Arguments are pid, and time to wait in seconds
# (forever by default).  FIXME: timeout not yet implemented
#
cmd_wait () {

  while true
  do
    cmd_state $1

    case "$RETVAL" in
      DONE      ) return ;;
      FAILED    ) return ;;
      CANCELED  ) return ;;
      NEW       )        ;;
      RUNNING   )        ;;
      SUSPENDED )        ;;
      UNKNOWN   )        ;;   # FIXME: should be an error?
      *         ) ERROR="NOK - invalid state '$RETVAL'"; return ;;  
    esac

    \sleep 1
  done
}


# --------------------------------------------------------------------
#
# get exit code
#
cmd_result () {
  verify_state $1 || return

  DIR="$BASE/$1"
  state=`\grep -e ' $' "$DIR/state" | \tail -n 1 | \tr -d ' '`

  if test "$state" != "DONE" -a "$state" != "FAILED" -a "$state" != "CANCELED"
  then 
    ERROR="job $1 in incorrect state ($state != DONE|FAILED|CANCELED)"
    return
  fi

  if ! test -r "$DIR/exit"
  then
    ERROR="job $1 in incorrect state -- no exit code available"
  fi

  RETVAL=`\cat "$DIR/exit"`
}


# --------------------------------------------------------------------
#
# suspend a running job
#
cmd_suspend () {
  verify_state $1 || return
  verify_pid   $1 || return

  DIR="$BASE/$1"
  state=`\grep -e ' $' "$DIR/state" | \tail -n 1 | \tr -d ' '`
  rpid=`\cat "$DIR/rpid"`

  if ! test "$state" = "RUNNING"
  then
    ERROR="job $1 in incorrect state ($state != RUNNING)"
    return
  fi

  \touch "$DIR/suspended"
  RETVAL=`/bin/kill -STOP $rpid 2>&1`
  ECODE=$?

  if test "$ECODE" = "0"
  then
    \printf "SUSPENDED \n" >>  "$DIR/state"
    \printf "$state \n"    >   "$DIR/state.susp"
    RETVAL="$1 suspended"
  else
    \rm -f   "$DIR/suspended"
    ERROR="suspend failed ($ECODE): $RETVAL"
  fi

}


# --------------------------------------------------------------------
#
# resume a suspended job
#
cmd_resume () {
  verify_state $1 || return
  verify_pid   $1 || return

  DIR="$BASE/$1"
  state=`\grep -e ' $' "$DIR/state" | \tail -n 1 | \tr -d ' '`
  rpid=`\cat "$DIR/rpid"`

  if ! test "$state" = "SUSPENDED"
  then
    ERROR="job $1 in incorrect state ($state != SUSPENDED)"
    return
  fi

  \touch   "$DIR/resumed"
  RETVAL=`/bin/kill -CONT $rpid 2>&1`
  ECODE=$?

  if test "$ECODE" = "0"
  then
    test -s "$DIR/state.susp" || \printf "RUNNING \n" >  "$DIR/state.susp"
    \cat    "$DIR/state.susp"                         >> "$DIR/state"
    \rm  -f "$DIR/state.susp"
    RETVAL="$1 resumed"
  else
    \rm  -f "$DIR/resumed"
    ERROR="resume failed ($ECODE): $RETVAL"
  fi

}


# --------------------------------------------------------------------
#
# kill a job, and set state to canceled
#
cmd_cancel () {
  verify_state $1 || return
  verify_pid   $1 || return

  DIR="$BASE/$1"


  rpid=`\cat "$DIR/rpid"`
  mpid=`\cat "$DIR/mpid"`

  # first kill monitor, so that it does not interfer with state management
  /bin/kill -TERM $mpid 2>/dev/null
  /bin/kill -KILL $mpid 2>/dev/null

  # now make sure that job did not reach final state before monitor died
  state=`\grep -e ' $' "$DIR/state" | \tail -n 1 | \tr -d ' '`
  if test "$state" = "FAILED" -o "$state" = "DONE" -o "$state" = "CANCELED"
  then
    ERROR="job $1 in incorrect state ('$state' = 'DONE|FAILED|CANCELED')"
    return
  fi

  # now kill the job process group, and to be sure also the job shell
  /bin/kill -TERM $KILL_DASHES -$mpid # this is the important one...
  /bin/kill -KILL $KILL_DASHES -$mpid 2>/dev/null
  /bin/kill -TERM               $rpid 2>/dev/null
  /bin/kill -KILL               $rpid 2>/dev/null

  # FIXME: how can we check for success?  ps?
  \printf "CANCELED \n" >> "$DIR/state"
  RETVAL="$1 canceled"
}


# --------------------------------------------------------------------
#
# feed given string to job's stdin stream
#
cmd_stdin () {
  verify_in $1 || return

  DIR="$BASE/$1"
  shift
  \printf "$*" >> "$DIR/in"
  RETVAL="stdin refreshed"
}


# --------------------------------------------------------------------
#
# print uuencoded string of job's stdout
#
cmd_stdout () {
  verify_out $1 || return

  DIR="$BASE/$1"
  RETVAL=`uuencode "$DIR/out" "/dev/stdout"`
}


# --------------------------------------------------------------------
#
# print uuencoded string of job's stderr
#
cmd_stderr () {
  verify_err $1 || return

  DIR="$BASE/$1"
  RETVAL=`uuencode "$DIR/err" "/dev/stdout"`
}


# --------------------------------------------------------------------
#
# list all job IDs
#
cmd_list () {
  RETVAL=`(\cd "$BASE" ; \ls -C1 -d */ 2>/dev/null) | \cut -f 1 -d '/'`
}


# --------------------------------------------------------------------
#
# purge working directories of given jobs (all non-final jobs as default)
#
cmd_purge () {

  if ! test -z "$1"
  then
    DIR="$BASE/$1"
    \rm -rf "$DIR"
    RETVAL="purged $1"
  else
    for d in `\grep -l -e 'DONE' -e 'FAILED' -e 'CANCELED' "$BASE"/*/state 2>/dev/null`
    do
      dir=`dirname "$d"`
      id=`basename "$dir"`
      \rm -rf "$BASE/$id" >/dev/null 2>&1
    done
    RETVAL="purged finished jobs"
  fi
}


# --------------------------------------------------------------------
#
# quit this script gracefully
#
cmd_quit () {

  if test "$1" = "TIMEOUT"
  then
    \printf "IDLE TIMEOUT\n"
    \touch "$BASE/timed_out.$$"
    EXIT_VAL=2
  else
    \touch "$BASE/quit.$$"
  fi

  # kill idle checker
  /bin/kill $1 >/dev/null 2>&1
  \rm -f "$BASE/idle.$$"

  # clean bulk file and other temp files
  \rm -f bulk.$$

  # restore shell echo
  \stty echo    >/dev/null 2>&1
  \stty echonl  >/dev/null 2>&1

  exit $EXIT_VAL
}


# --------------------------------------------------------------------
#
# main even loop -- wait for incoming command lines, and react on them
#
listen() {

  # we need our home base cleaned
  test -d "$BASE" || \mkdir -p  "$BASE"  || exit 1
  \touch  "$BASE/bulk.$$"
  \rm  -f "$BASE/bulk.$$"
  \touch  "$BASE/bulk.$$"

  # make sure we get killed when idle
  idle_checker $$ 1>/dev/null 2>/dev/null 3</dev/null &
  IDLE=$!

  # report our own pid
  if ! test -z $1; then
    \printf "PID: $$\n" # FIXME: this should be $1
  fi

  # prompt for commands...
  \printf "PROMPT-0->\n"

  # and read those from stdin
  OLDIFS=$IFS
  IFS=
  while \read -r CMD ARGS
  do

    # check if we start or finish a bulk
    case $CMD in
      BULK     ) IN_BULK=1
                 BULK_ERROR="OK"
                 BULK_EXITVAL="0"
                 ;;
      BULK_RUN ) IN_BULK=""
                 \printf "BULK_EVAL\n" >> "$BASE/bulk.$$"
                 ;;
      *        ) \echo   "$CMD $ARGS"  >> "$BASE/bulk.$$"
                 ;;
    esac

    if ! test -z "$IN_BULK"
    then
      # continue to collect bulk commands
      continue
    fi

    # no more bulk collection (if there ever was any) -- execute the collected
    # command lines.
    IFS=$OLDIFS
    while \read -r CMD ARGS
    do

      # reset err state for each command
      ERROR="OK"
      RETVAL=""

      # simply invoke the right function for each command, or complain if command
      # is not known
      case $CMD in
        RUN       ) cmd_run     "$ARGS"  ;;
        LRUN      ) cmd_lrun    "$ARGS"  ;;
        SUSPEND   ) cmd_suspend "$ARGS"  ;;
        RESUME    ) cmd_resume  "$ARGS"  ;;
        CANCEL    ) cmd_cancel  "$ARGS"  ;;
        RESULT    ) cmd_result  "$ARGS"  ;;
        STATE     ) cmd_state   "$ARGS"  ;;
        STATS     ) cmd_stats   "$ARGS"  ;;
        WAIT      ) cmd_wait    "$ARGS"  ;;
        STDIN     ) cmd_stdin   "$ARGS"  ;;
        STDOUT    ) cmd_stdout  "$ARGS"  ;;
        STDERR    ) cmd_stderr  "$ARGS"  ;;
        LIST      ) cmd_list    "$ARGS"  ;;
        PURGE     ) cmd_purge   "$ARGS"  ;;
        QUIT      ) cmd_quit    "$IDLE"  ;;
        NOOP      ) ERROR="NOOP"         ;;
        BULK_EVAL ) ERROR="$BULK_ERROR"
                    RETVAL="BULK COMPLETED"
                    test "$ERROR" = OK || false
                    ;;
        *         ) RETVAL=$($CMD $ARGS 2>&1) || ERROR="NOK - command '$CMD' failed" ;;
      esac

      EXITVAL=$?

      # the called function will report state and results in 'ERROR' and 'RETVAL'
      if test "$ERROR" = "OK"; then
        \printf "OK\n"
        \printf "$RETVAL\n"
      elif test "$ERROR" = "NOOP"; then
        # nothing
        true
      else
        \printf "ERROR\n"
        \printf "$ERROR\n"
        \printf "$RETVAL\n"
        BULK_ERROR="NOK - bulk error '$ERROR'"  # a single error spoils the bulk
        BULK_EXITVAL="$EXITVAL"
      fi

      # we did hard work - make sure we are not getting killed for idleness!
      \rm -f "$BASE/idle.$$"

      # well done - prompt for next command (even in bulk mode, for easier
      # parsing and EXITVAL communication)
      \printf "PROMPT-$EXITVAL->\n"

    # closing thye read loop for the bulk data file
    done < "$BASE/bulk.$$"

    # empty the bulk data file
    \rm -f "$BASE/bulk.$$"

    # next main loop read needs IFS reset again
    OLDIFS=$IFS
    IFS=

  done
}


# --------------------------------------------------------------------
#
# run the main loop -- that will live forever, until a 'QUIT' command is
# encountered.
#
# The first arg to wrapper.sh is the id of the spawning shell, which we need to
# report, if given
#
\stty -echo   2> /dev/null
\stty -echonl 2> /dev/null

# FIXME: this leads to timing issues -- disabled for benchmarking
# if test "$PURGE_ON_START" = "True"
# then
#   cmd_purge
# fi

create_monitor
listen $1
#
# --------------------------------------------------------------------

# vim: tabstop=2 expandtab shiftwidth=2 softtabstop=2

