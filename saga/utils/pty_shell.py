
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import re
import os
import sys
import errno

import saga.utils.misc              as sumisc
import radical.utils.logger         as rul

import saga.utils.pty_shell_factory as supsf
import saga.url                     as surl
import saga.exceptions              as se
import saga.session                 as ss

import pty_exceptions               as ptye


# ------------------------------------------------------------------------------
#
_PTY_TIMEOUT = 2.0

# ------------------------------------------------------------------------------
#
# iomode flags
#
IGNORE   = 0    # discard stdout / stderr
MERGED   = 1    # merge stdout and stderr
SEPARATE = 2    # fetch stdout and stderr individually (one more hop)
STDOUT   = 3    # fetch stdout only, discard stderr
STDERR   = 4    # fetch stderr only, discard stdout


# --------------------------------------------------------------------
#
class PTYShell (object) :
    """
    This class wraps a shell process and runs it as a :class:`PTYProcess`.  The
    user of this class can start that shell, and run arbitrary commands on it.

    The shell to be run is expected to be POSIX compliant (bash, dash, sh, ksh
    etc.) -- in particular, we expect the following features:
    ``$?``,
    ``$!``,
    ``$#``,
    ``$*``,
    ``$@``,
    ``$$``,
    ``$PPID``,
    ``>&``,
    ``>>``,
    ``>``,
    ``<``,
    ``|``,
    ``||``,
    ``()``,
    ``&``,
    ``&&``,
    ``wait``,
    ``kill``,
    ``nohup``,
    ``shift``,
    ``export``,
    ``PS1``, and
    ``PS2``.

    Note that ``PTYShell`` will change the shell prompts (``PS1`` and ``PS2``),
    to simplify output parsing.  ``PS2`` will be empty, ``PS1`` will be set
    ``PROMPT-$?->`` -- that way, the prompt will report the exit value of the
    last command, saving an extra roundtrip.  Users of this class should be
    careful when setting other prompts -- see :func:`set_prompt` for more
    details.

    Usage Example::

        # start the shell, find its prompt.  
        self.shell = saga.utils.pty_shell.PTYShell ("ssh://user@remote.host.net/", contexts, self._logger)

        # run a simple shell command, merge stderr with stdout.  $$ is the pid
        # of the shell instance.
        ret, out, _ = self.shell.run_sync ("mkdir -p /tmp/data.$$/" )

        # check if mkdir reported success
        if  ret != 0 :
            raise saga.NoSuccess ("failed to prepare base dir (%s)(%s)" % (ret, out))

        # stage some data from a local string variable into a file on the remote system
        self.shell.stage_to_remote (src = pbs_job_script, 
                                    tgt = "/tmp/data.$$/job_1.pbs")

        # check size of staged script (this is actually done on PTYShell level
        # already, with no extra hop):
        ret, out, _ = self.shell.run_sync ("stat -c '%s' /tmp/data.$$/job_1.pbs" )
        if  ret != 0 :
            raise saga.NoSuccess ("failed to check size (%s)(%s)" % (ret, out))

        assert (len(pbs_job_script) == int(out))


    **Data Staging and Data Management:**
    

    The PTYShell class does not only support command execution, but also basic
    data management: for SSH based shells, it will create a tunneled scp/sftp
    connection for file staging.  Other data management operations (mkdir, size,
    list, ...) are executed either as shell commands, or on the scp/sftp channel
    (if possible on the data channel, to keep the shell pty free for concurrent
    command execution).  Ssh tunneling is implemented via ssh.v2 'ControlMaster'
    capabilities (see `ssh_config(5)`).
    
    For local shells, PTYShell will create an additional shell pty for data
    management operations.  


    **Asynchronous Notifications:**

    A third pty process will be created for asynchronous notifications.  For
    that purpose, the shell started on the first channel will create a named
    pipe, at::

      $HOME/.saga/adaptors/shell/async.$$

    ``$$`` here represents the pid of the shell process.  It will also set the
    environment variable ``SAGA_ASYNC_PIPE`` to point to that named pipe -- any
    application running on the remote host can write event messages to that
    pipe, which will be available on the local end (see below).  `PTYShell`
    leaves it unspecified what format those messages have, but messages are
    expected to be separated by newlines.
    
    An adaptor using `PTYShell` can subscribe for messages via::

      self.pty_shell.subscribe (callback)

    where callback is a Python callable.  PTYShell will listen on the event
    channel *in a separate thread* and invoke that callback on any received
    message, passing the message text (sans newline) to the callback.

    An example usage: the command channel may run the following command line::

      ( sh -c 'sleep 100 && echo "job $$ done" > $SAGA_ASYNC_PIPE" \
                         || echo "job $$ fail" > $SAGA_ASYNC_PIPE" ) &

    which will return immediately, and send a notification message at job
    completion.

    Note that writes to named pipes are not atomic.  From POSIX:

    ``A write is atomic if the whole amount written in one operation is not
    interleaved with data from any other process. This is useful when there are
    multiple writers sending data to a single reader. Applications need to know
    how large a write request can be expected to be performed atomically. This
    maximum is called {PIPE_BUF}. This volume of IEEE Std 1003.1-2001 does not
    say whether write requests for more than {PIPE_BUF} bytes are atomic, but
    requires that writes of {PIPE_BUF} or fewer bytes shall be atomic.`

    Thus the user is responsible for ensuring that either messages are smaller
    than *PIPE_BUF* bytes on the remote system (usually at least 1024, on Linux
    usually 4096), or to lock the pipe on larger writes.


    **Automated Restart, Timeouts:**

    For timeout and restart semantics, please see the documentation to the
    underlying :class:`saga.utils.pty_process.PTYProcess` class.

    """

    # TODO: 
    #   - on client shell activitites, also mark the master as active, to
    #     avoid timeout garbage collection.
    #   - use ssh mechanisms for master timeout (and persist), as custom
    #     mechanisms will interfere with gc_timout.

    # ----------------------------------------------------------------
    #
    def __init__ (self, url, session=None, logger=None, init=None, opts={}) :

        if  None != logger  : self.logger  = logger
        else                : self.logger  = rul.getLogger ('saga', 'PTYShell') 

        self.logger.debug ("PTYShell init %s" % self)

        self.url         = url      # describes the shell to run
        self.init        = init     # call after reconnect
        self.opts        = opts     # options...
        self.latency     = 0.0      # set by factory

        self.prompt      = None
        self.prompt_re   = None
        self.initialized = False

        # we need a local dir for file staging caches.  At this point we use
        # $HOME, but should make this configurable (FIXME)
        self.base = os.environ['HOME'] + '/.saga/adaptors/shell/'

        try:
            os.makedirs (self.base)

        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir (self.base):
                pass
            else: 
                raise se.NoSuccess ("could not create staging dir: %s" % e)

        
        self.factory    = supsf.PTYShellFactory   ()
        self.pty_info   = self.factory.initialize (url, session, self.logger)
        self.pty_shell  = self.factory.run_shell  (self.pty_info)

        self.initialize ()


    # ----------------------------------------------------------------
    #
    def __del__ (self) :

        self.logger.debug ("PTYShell del  %s" % self)
        self.finalize (kill_pty=True)


    # ----------------------------------------------------------------
    #
    def initialize (self) :
        """ initialize the shell connection.  """

        with self.pty_shell.rlock :

            if  self.initialized :
                self.logger.warn ("initialization race")
                return


            # run a POSIX compatible shell, usually /bin/sh, in interactive mode
            # also, turn off tty echo
            command_shell = "exec /bin/sh -i"

            # use custom shell if so requested
            if  'shell' in self.opts and self.opts['shell'] :
                command_shell = "exec %s" % self.opts['shell']
                self.logger.info ("custom  command shell: %s" % command_shell)


            self.logger.debug    ("running command shell: %s" % command_shell)
            self.pty_shell.write ("stty -echo ; %s\n"         % command_shell)

            # make sure this worked, and that we find the prompt. We use
            # a versatile prompt pattern to account for the custom shell case.
            self.find (["^(.*[\$#%>])\s*$"])

            # make sure this worked, and that we find the prompt. We use
            # a versatile prompt pattern to account for the custom shell case.
            try :
                # set and register new prompt
                self.run_async  ("unset PROMPT_COMMAND ; "
                                     + "PS1='PROMPT-$?->'; "
                                     + "PS2=''; "
                                     + "export PS1 PS2 2>&1 >/dev/null\n")
                self.set_prompt (new_prompt="PROMPT-(\d+)->$")

                self.logger.debug ("got new shell prompt")

            except Exception as e :
                raise se.NoSuccess ("Shell startup on target host failed: %s" % e)


            try :
                # got a command shell, finally!
                # for local shells, we now change to the current working
                # directory.  Remote shells will remain in the default pwd
                # (usually $HOME).
                if  sumisc.host_is_local (surl.Url(self.url).host) :
                    pwd = os.getcwd ()
                    self.run_sync ('cd %s' % pwd)
            except Exception as e :
                # We will ignore any errors.
                self.logger.warning ("local cd to %s failed" % pwd)
                
                

            self.initialized = True


    # ----------------------------------------------------------------
    #
    def finalize (self, kill_pty = False) :

        try :
            if  kill_pty and self.pty_shell :
                with self.pty_shell.rlock :
                    self.pty_shell.finalize ()

        except Exception as e :
            pass



    # ----------------------------------------------------------------
    #
    def alive (self, recover=False) :
        """
        The shell is assumed to be alive if the shell processes lives.
        Attempt to restart shell if recover==True
        """

        with self.pty_shell.rlock :

            try :
                return self.pty_shell.alive (recover)

            except Exception as e :
                raise ptye.translate_exception (e)


    # ----------------------------------------------------------------
    #
    def find_prompt (self) :
        """
        If run_async was called, a command is running on the shell.  find_prompt
        can be used to collect its output up to the point where the shell prompt
        re-appears (i.e. when the command finishes).


        Note that this method blocks until the command finishes.  Future
        versions of this call may add a timeout parameter.
        """

        with self.pty_shell.rlock :

            try :

                match = None
                fret  = None

                while fret == None :
                    fret, match = self.pty_shell.find ([self.prompt], _PTY_TIMEOUT)
                
              # self.logger.debug  ("find prompt '%s' in '%s'" % (self.prompt, match))
                ret, txt = self._eval_prompt (match)

                return (ret, txt)

            except Exception as e :
                raise ptye.translate_exception (e)


    # ----------------------------------------------------------------
    #
    def find (self, patterns, timeout=-1) :
        """
        Note that this method blocks until pattern is found in the shell I/O.
        """

        with self.pty_shell.rlock :

            try :
                return self.pty_shell.find (patterns, timeout=timeout)

            except Exception as e :
                raise ptye.translate_exception (e)


    # ----------------------------------------------------------------
    #
    def set_prompt (self, new_prompt) :
        """
        :type  new_prompt:  string 
        :param new_prompt:  a regular expression matching the shell prompt

        The new_prompt regex is expected to be a regular expression with one set
        of catching brackets, which MUST return the previous command's exit
        status.  This method will send a newline to the client, and expects to
        find the prompt with the exit value '0'.

        As a side effect, this method will discard all previous data on the pty,
        thus effectively flushing the pty output.  

        By encoding the exit value in the command prompt, we safe one roundtrip.
        The prompt on Posix compliant shells can be set, for example, via::

          PS1='PROMPT-$?->'; export PS1

        The newline in the example above allows to nicely anchor the regular
        expression, which would look like::

          PROMPT-(\d+)->$

        The regex is compiled with 're.DOTALL', so the dot character matches
        all characters, including line breaks.  Be careful not to match more
        than the exact prompt -- otherwise, a prompt search will swallow stdout
        data.  For example, the following regex::

          PROMPT-(.+)->$

        would capture arbitrary strings, and would thus match *all* of::

          PROMPT-0->ls
          data/ info
          PROMPT-0->

        and thus swallow the ls output...

        Note that the string match *before* the prompt regex is non-gready -- if
        the output contains multiple occurrences of the prompt, only the match
        up to the first occurence is returned.
        """

        with self.pty_shell.rlock :

            old_prompt     = self.prompt
            self.prompt    = new_prompt
            self.prompt_re = re.compile ("^(.*?)%s\s*$" % self.prompt, re.DOTALL)

            retries  = 0
            triggers = 0

            while True :

                try :
                    # make sure we have a non-zero waiting delay (default to
                    # 1 second)
                    delay = 10 * self.latency
                    if  not delay :
                        delay = 1.0

                    # FIXME: how do we know that _PTY_TIMOUT suffices?  In particular if
                    # we actually need to flush...
                    fret, match = self.pty_shell.find ([self.prompt], delay)

                    if  fret == None :
                    
                        retries += 1
                        if  retries > 10 :
                            self.prompt = old_prompt
                            raise se.BadParameter ("Cannot use new prompt, parsing failed (10 retries)")

                        self.pty_shell.write ("\n")
                        self.logger.debug  ("sent prompt trigger again (%d)" % retries)
                        triggers += 1
                        continue


                    # found a match -- lets see if this is working now...
                    ret, _ = self._eval_prompt (match)

                    if  ret != 0 :
                        self.prompt = old_prompt
                        raise se.BadParameter ("could not parse exit value (%s)" \
                                            % match)

                    # prompt looks valid...
                    break

                except Exception as e :
                    self.prompt = old_prompt
                    raise ptye.translate_exception (e, "Could not set shell prompt")


            # got a valid prompt -- but we have to sync the output again in
            # those cases where we had to use triggers to actually get the
            # prompt
            if triggers > 0 :
                self.run_async ('printf "SYNCHRONIZE_PROMPT\n"')

                # FIXME: better timout value?
                fret, match = self.pty_shell.find (["SYNCHRONIZE_PROMPT"], timeout=1.0)  

                if  fret == None :
                    # not find prompt after blocking?  BAD!  Restart the shell
                    self.finalize (kill_pty=True)
                    raise se.NoSuccess ("Could not synchronize prompt detection")

                self.find_prompt ()



    # ----------------------------------------------------------------
    #
    def _eval_prompt (self, data, new_prompt=None) :
        """
        This method will match the given data against the current prompt regex,
        and expects to find an integer as match -- which is then returned, along
        with all leading data, in a tuple
        """

        with self.pty_shell.rlock :

            try :

                prompt    = self.prompt
                prompt_re = self.prompt_re

                if  new_prompt :
                    prompt    = new_prompt
                    prompt_re = re.compile ("^(.*)%s\s*$" % prompt, re.DOTALL)


                result = None
                if  not data :
                    raise se.NoSuccess ("cannot not parse prompt (%s), invalid data (%s)" \
                                     % (prompt, data))

                result = prompt_re.match (data)

                if  not result :
                    self.logger.debug  ("could not parse prompt (%s) (%s)" % (prompt, data))
                    raise se.NoSuccess ("could not parse prompt (%s) (%s)" % (prompt, data))

                if  len (result.groups ()) != 2 :
                    self.logger.debug  ("prompt does not capture exit value (%s)" % prompt)
                    raise se.NoSuccess ("prompt does not capture exit value (%s)" % prompt)

                txt =     result.group (1)
                ret = int(result.group (2)) 

                # if that worked, we can permanently set new_prompt
                if  new_prompt :
                    self.set_prompt (new_prompt)

                return (ret, txt)

            except Exception as e :
                
                raise ptye.translate_exception (e, "Could not eval prompt")




    # ----------------------------------------------------------------
    #
    def run_sync (self, command, iomode=None, new_prompt=None) :
        """
        Run a shell command, and report exit code, stdout and stderr (all three
        will be returned in a tuple).  The call will block until the command
        finishes (more exactly, until we find the prompt again on the shell's
        I/O stream), and cannot be interrupted.

        :type  command: string
        :param command: shell command to run.  
        
        :type  iomode:  enum
        :param iomode:  Defines how stdout and stderr are captured.  

        :type  new_prompt:  string 
        :param new_prompt:  regular expression matching the prompt after
        command succeeded.

        We expect the ``command`` to not to do stdio redirection, as this is we want
        to capture that separately.  We *do* allow pipes and stdin/stdout
        redirection.  Note that SEPARATE mode will break if the job is run in
        the background

        
        The following iomode values are valid:

          * *IGNORE:*   both stdout and stderr are discarded, `None` will be
                        returned for each.
          * *MERGED:*   both streams will be merged and returned as stdout; 
                        stderr will be `None`.  This is the default.
          * *SEPARATE:* stdout and stderr will be captured separately, and
                        returned individually.  Note that this will require 
                        at least one more network hop!  
          * *STDOUT:*   only stdout is captured, stderr will be `None`.
          * *STDERR:*   only stderr is captured, stdout will be `None`.
          * *None:*     do not perform any redirection -- this is effectively
                        the same as `MERGED`

        If any of the requested output streams does not return any data, an
        empty string is returned.

        
        If the command to be run changes the prompt to be expected for the
        shell, the ``new_prompt`` parameter MUST contain a regex to match the
        new prompt.  The same conventions as for set_prompt() hold -- i.e. we
        expect the prompt regex to capture the exit status of the process.
        """

        with self.pty_shell.rlock :

            # we expect the shell to be in 'ground state' when running a syncronous
            # command -- thus we can check if the shell is alive before doing so,
            # and restart if needed
            if not self.pty_shell.alive (recover=True) :
                raise se.IncorrectState ("Can't run command -- shell died:\n%s" \
                                      % self.pty_shell.autopsy ())

            try :

                command = command.strip ()
                if command.endswith ('&') :
                    raise se.BadParameter ("run_sync can only run foreground jobs ('%s')" \
                                        % command)

                redir = ""
                _err  = "/tmp/saga-python.ssh-job.stderr.$$"

                if  iomode == IGNORE :
                    redir  =  " 1>>/dev/null 2>>/dev/null"

                if  iomode == MERGED :
                    redir  =  " 2>&1"

                if  iomode == SEPARATE :
                    redir  =  " 2>%s" % _err

                if  iomode == STDOUT :
                    redir  =  " 2>/dev/null"

                if  iomode == STDERR :
                    redir  =  " 2>&1 1>/dev/null"

                if  iomode == None :
                    redir  =  ""

                self.logger.debug    ('run_sync: %s%s'   % (command, redir))
                self.pty_shell.write (          "%s%s\n" % (command, redir))


                # If given, switch to new prompt pattern right now...
                prompt = self.prompt
                if  new_prompt :
                    prompt = new_prompt

                # command has been started - now find prompt again.  
                fret, match = self.pty_shell.find ([prompt], timeout=-1.0)  # blocks

                if  fret == None :
                    # not find prompt after blocking?  BAD!  Restart the shell
                    self.finalize (kill_pty=True)
                    raise se.IncorrectState ("run_sync failed, no prompt (%s)" % command)


                ret, txt = self._eval_prompt (match, new_prompt)

                stdout = None
                stderr = None

                if  iomode == IGNORE :
                    pass

                if  iomode == MERGED :
                    stdout =  txt

                if  iomode == SEPARATE :
                    stdout =  txt

                    self.pty_shell.write ("cat %s\n" % _err)
                    fret, match = self.pty_shell.find ([self.prompt], timeout=-1.0)  # blocks

                    if  fret == None :
                        # not find prompt after blocking?  BAD!  Restart the shell
                        self.finalize (kill_pty=True)
                        raise se.IncorrectState ("run_sync failed, no prompt (%s)" \
                                              % command)

                    _ret, _stderr = self._eval_prompt (match)
                    if  _ret :
                        raise se.IncorrectState ("run_sync failed, no stderr (%s: %s)" \
                                              % (_ret, _stderr))
                    stderr =  _stderr


                if  iomode == STDOUT :
                    stdout =  txt

                if  iomode == STDERR :
                    stderr =  txt

                if  iomode == None :
                    stdout =  txt

                return (ret, stdout, stderr)

            except Exception as e :
                raise ptye.translate_exception (e)


    # ----------------------------------------------------------------
    #
    def run_async (self, command) :
        """
        Run a shell command, but don't wait for prompt -- just return.  It is up
        to caller to eventually search for the prompt again (see
        :func:`find_prompt`.  Meanwhile, the caller can interact with the called
        command, via the I/O channels.

        :type  command: string
        :param command: shell command to run.  

        For async execution, we don't care if the command is doing i/o redirection or not.
        """

        with self.pty_shell.rlock :

            # we expect the shell to be in 'ground state' when running an asyncronous
            # command -- thus we can check if the shell is alive before doing so,
            # and restart if needed
            if not self.pty_shell.alive (recover=True) :
                raise se.IncorrectState ("Cannot run command:\n%s" \
                                      % self.pty_shell.autopsy ())

            try :
                command = command.strip ()
                self.send ("%s\n" % command)

            except Exception as e :
                raise ptye.translate_exception (e)


    # ----------------------------------------------------------------
    #
    def send (self, data) :
        """
        send data to the shell.  No newline is appended!
        """

        with self.pty_shell.rlock :

            if not self.pty_shell.alive (recover=False) :
                raise se.IncorrectState ("Cannot send data:\n%s" \
                                      % self.pty_shell.autopsy ())

            try :
                self.pty_shell.write ("%s" % data)

            except Exception as e :
                raise ptye.translate_exception (e)

    # ----------------------------------------------------------------
    #
    def write_to_remote (self, src, tgt) :
        """
        :type  src: string
        :param src: data to be staged into the target file

        :type  tgt: string
        :param tgt: path to target file to staged to
                    The tgt path is not an URL, but expected to be a path
                    relative to the shell's URL.

        The content of the given string is pasted into a file (specified by tgt)
        on the remote system.  If that file exists, it is overwritten.
        A NoSuccess exception is raised if writing the file was not possible
        (missing permissions, incorrect path, etc.).
        """

        try :

            # FIXME: make this relative to the shell's pwd?  Needs pwd in
            # prompt, and updating pwd state on every find_prompt.

            # first, write data into a tmp file
            fname   = self.base + "/staging.%s" % id(self)
            fhandle = open (fname, 'wb')
            fhandle.write  (src)
            fhandle.flush  ()
            fhandle.close  ()

            ret = self.factory.run_copy_to (self.pty_info, fname, tgt)

            os.remove (fname)

            return ret

        except Exception as e :
            raise ptye.translate_exception (e)


    # ----------------------------------------------------------------
    #
    def read_from_remote (self, src) :
        """
        :type  src: string
        :param src: path to source file to staged from
                    The src path is not an URL, but expected to be a path
                    relative to the shell's URL.
        """

        try :
            # FIXME: make this relative to the shell's pwd?  Needs pwd in
            # prompt, and updating pwd state on every find_prompt.

            # first, write data into a tmp file
            fname   = self.base + "/staging.%s" % id(self)

            _ = self.factory.run_copy_from (self.pty_info, src, fname)

            fhandle = open (fname, 'r')
            out = fhandle.read  ()
            fhandle.close  ()

            os.remove (fname)

            return out

        except Exception as e :
            raise ptye.translate_exception (e)


    # ----------------------------------------------------------------
    #
    def stage_to_remote (self, src, tgt, cp_flags="") :
        """
        :type  src: string
        :param src: path of local source file to be stage from.
                    The tgt path is not an URL, but expected to be a path
                    relative to the current working directory.

        :type  tgt: string
        :param tgt: path to target file to stage to.
                    The tgt path is not an URL, but expected to be a path
                    relative to the shell's URL.
        """

        # FIXME: make this relative to the shell's pwd?  Needs pwd in
        # prompt, and updating pwd state on every find_prompt.

        try :
            return self.factory.run_copy_to (self.pty_info, src, tgt, cp_flags)

        except Exception as e :
            raise ptye.translate_exception (e)

    # ----------------------------------------------------------------
    #
    def stage_from_remote (self, src, tgt, cp_flags="") :
        """
        :type  src: string
        :param tgt: path to source file to stage from.
                    The tgt path is not an URL, but expected to be a path
                    relative to the shell's URL.

        :type  tgt: string
        :param src: path of local target file to stage to.
                    The tgt path is not an URL, but expected to be a path
                    relative to the current working directory.
        """

        # FIXME: make this relative to the shell's pwd?  Needs pwd in
        # prompt, and updating pwd state on every find_prompt.

        try :
            return self.factory.run_copy_from (self.pty_info, src, tgt, cp_flags)

        except Exception as e :
            raise ptye.translate_exception (e)





