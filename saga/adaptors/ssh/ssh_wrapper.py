
# --------------------------------------------------------------------
# server side job management script
_PROMPT     = "CMD"
_PROMPT_OK  = "OK"
_PROMPT_ERR = "ERROR"
_MANAGER    = '''#!/bin/sh

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
  
  echo "CMD"

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
    echo "CMD"
  done
}
# --------------------------------------------------------------------
listen
# --------------------------------------------------------------------

'''

