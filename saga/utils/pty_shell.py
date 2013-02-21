
import re
import os
import sys

import saga.utils.pty_process
import saga.utils.logger

_PTY_TIMEOUT = 2.0
_SCHEMAS     = ['ssh', 'gsissh', 'fork', 'shell']

IGNORE   = 0    # discard stdout / stderr
MERGED   = 1    # merge stdout and stderr
SEPARATE = 2    # fetch stdout and stderr individually (one more hop)
STDOUT   = 3    # fetch stdout only, discard stderr
STDERR   = 4    # fetch stderr only, discard stdout

# ssh options:
#   -e none         : no escape character
#   -M              : master mode for connection sharing
#   -S control_path : slave mode for connection sharing
#   -t              : force pty allocation
#   -x              : disable x11 forwarding
#   
#   ServerAliveInterval
#   CheckHostIP no
#   ConnectTimeout
#   ControlMaster  yes | no | no ...
#   ControlPath    $BASE/ssh_control_%n_%p.$$.sock
#                  %r (remote id)? would need inspection
#   ControlPersist 10   : close master after 10 seconds idle
#   EscapeChar     none : transparent for binary data
#   TCPKeepAlive   yes  : detect connection failure
#
#   
#


# --------------------------------------------------------------------
#
class PTYShell (object) :
    """
    This class wraps a shell process and runs it as a :class:`PTYProcess`.  The
    user of this class can start that shell, and run arbitrary commands on it.

    The shell to be run is expected to be POSIX compliant (bash, csh, sh, zsh
    etc) -- in particular, we expect the following features:
    ``$?``,
    ``$!``,
    ``$*``,
    ``$#``,
    ``$@``,
    ``$PPID``,
    ``>&``,
    ``>>``,
    ``>``,
    ``<``,
    ``2>&1``,
    ``|``,
    ``||``,
    ``&&``,
    ``wait``,
    ``kill``,
    ``nohup``,
    ``shift``,
    ``PS1``, and
    ``PS2``.

    Note that ``PTYShell`` will change the shell prompts (``PS1`` and ``PS2``),
    to simplify output parsing.  ``PS2`` will be empty, ``PS1`` will be set
    ``PROMPT-$?->\\n`` -- that way, the prompt will report the exit value of the
    last command, saving an extra roundtrip.  Users of this class should be
    careful when setting other prompts -- see :func:`set_prompt` for more
    details.


    Data Staging and Data Management:
    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

    The PTYShell class does not only support commnd execution, but also basic
    data management: for SSH based shells, it will create a tunneled scp/sftp
    connection for file staging.  Other data management operations (mkdir, size,
    list, ...) are executed either as shell commands, or on the scp/sftp channel
    (if possible on the data channel, to keep the shell pty free for concurrent
    command execution).  Ssh tunneling is implemented via ssh.v2 'ControlMaster'
    capabilities (see `ssh_config(5)`).
    
    For local shells, PTYShell will create an additional shell pty for data
    management operations.  

    Future versions of this class may support additional command channels for
    more efficient asynchronous operations.


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
        self.shell.stage_to_file (src = pbs_job_script, 
                                  tgt = "/tmp/data.$$/job_1.pbs")

        # check size of staged script
        ret, out, _ = self.shell.run_sync ("stat -c '%s' /tmp/data.$$/job_1.pbs" )
        if  ret != 0 :
            raise saga.NoSuccess ("failed to check size (%s)(%s)" % (ret, out))

        assert (len(pbs_job_script) == int(out))

    Known Problems:
    ^^^^^^^^^^^^^^^

    - If the ssh connection gets dropped on the remote end, the pty_process
      remains active on MacOS -- it does not seem to collect the dying child.

    """

    # ----------------------------------------------------------------
    #
    def __init__ (self, url, contexts=[], logger=None, init=None) :

        self.url       = url               # describes the shell to run
        self.contexts  = contexts          # get security tokens from these
        self.logger    = logger            # possibly log to here
        self.init      = init              # call after reconnect

        self.initialize_hook = None
        self.finalize_hook   = None
        
        # need a new logger?
        if not self.logger :
            self.logger = saga.utils.logger.getLogger ('PTYShell')

        schema  = self.url.schema.lower ()
        self.shell_type = ""

        self.shell_exe  = ""
        self.shell_user = ""
        self.shell_pass = ""
        self.shell_host = ""

        self.data_exe   = ""
        self.data_user  = ""
        self.data_pass  = ""
        self.data_host  = ""

        # on demand, we open a second channel for scp
        self.pty_shell = None
        self.pty_data  = None

        # find out what type of shell we have to deal with
        if  schema   == "ssh" :
            self.shell_type  =  "ssh"
            self.shell_exe   =  saga.utils.which.which ("ssh")
            self.data_exe    =  saga.utils.which.which ("scp")

        elif schema  == "gsissh" :
            self.shell_type  =  "ssh"
            self.shell_exe   =  saga.utils.which.which ("gsissh")
            self.data_exe    =  saga.utils.which.which ("gsiscp")

        elif schema  == "fork"  or \
             schema  == "shell" :
            self.shell_type  =  "sh"
            if  "SHELL" in os.environ :
                self.shell_exe =  saga.utils.which.which (os.environ["SHELL"])
            else :
                self.shell_exe =  saga.utils.which.which ("sh")

            self.data_exe = self.shell_exe

        else :
            raise saga.BadParameter._log (self.logger, \
            	  "PTYShell utility can only handle %s schema URLs, not %s" \
                  % (_SCHEMAS, schema))


        # make sure we have something to run
        if not self.shell_exe :
            raise saga.BadParameter._log (self.logger, \
            	  "adaptor cannot handle %s://, no shell exe found" % schema)


        # depending on type, create PTYProcess command line (args, env etc)
        #
        # We always set term=vt100 to avoid ansi-escape sequences in the prompt
        # and elsewhere.  Also, we have to make sure that the shell is an
        # interactive login shell, so that it interprets the users startup
        # files, and reacts on commands.
        if  self.shell_type == "ssh" :

            self.shell_env  =  "/usr/bin/env TERM=vt100 "  # avoid ansi escapes
            self.shell_args =  "-t "                       # force pty

            for context in self.contexts :

                if  context.type.lower () == "ssh" :
                    # ssh can handle user_id and user_key of ssh contexts
                    if  schema == "ssh" :
                        if  context.attribute_exists ("user_id") :
                            self.shell_user  = context.user_id
                        if  context.attribute_exists ("user_key") :
                            self.shell_args += "-i %s " % context.user_key

                if  context.type.lower () == "userpass" :
                    # FIXME: ssh should also be able to handle UserPass contexts
                    if  schema == "ssh" :
                        if  context.attribute_exists ("user_id") :
                            self.shell_user = context.user_id
                        if  context.attribute_exists ("user_pass") :
                            self.shell_pass = context.user_pass

                if  context.type.lower () == "gsissh" :
                    # gsissh can handle user_proxy of X509 contexts
                    # FIXME: also use cert_dir etc.
                    if  context.attribute_exists ("user_proxy") :
                        if  schema == "gsissh" :
                            self.shell_env = "X509_PROXY='%s' " % context.user_proxy

            # all ssh based shells allow for user_id from contexts -- but the
            # username given in the URL takes precedence
            if self.url.username :
                self.shell_user = self.url.username

            if self.shell_user :
                self.shell_args += "-l %s " % self.shell_user


            self.data_env  = self.shell_env
            self.data_args = self.shell_args
            self.data_host = self.shell_host

            # build the ssh command line
            self.shell_cmd = "%s %s %s %s" % (self.shell_env, 
                                              self.shell_exe,
                                              self.shell_args, 
                                              self.url.host)
            self.data_cmd  = "%s %s %s %s" % (self.shell_env, 
                                              self.data_exe,
                                              self.shell_args, 
                                              self.url.host)

        # a local shell
        elif self.shell_type == "sh" :
            # Make sure we have an interactive login shell w/o ansi escapes.
            # Note that we redirect the shell's stderr to stdout -- pty-process
            # does not expose stderr separately...
            self.shell_args =  "-l -i"
            self.shell_env  =  "/usr/bin/env TERM=vt100"
            self.shell_cmd  =  "%s %s %s" % (self.shell_env, 
                                             self.shell_exe,
                                             self.shell_args)

        # we got the shell command - now run it!
        self.logger.info ("job service opens pty for '%s'" % self.shell_cmd)
        self.pty_shell = saga.utils.pty_process.PTYProcess (self.shell_cmd, 
                                                            logger=self.logger)

        self.pty_shell.set_initialize_hook (self.initialize)
        self.pty_shell.set_finalize_hook   (self.finalize)

        self.initialize ()


    # ----------------------------------------------------------------
    #
    def __del__ (self) :

        self.finalize (kill_pty=True)


    # ----------------------------------------------------------------------
    #
    def set_initialize_hook (self, initialize_hook) :
        self.initialize_hook = initialize_hook


    # ----------------------------------------------------------------------
    #
    def set_finalize_hook (self, finalize_hook) :
        self.finalize_hook = finalize_hook


    # ----------------------------------------------------------------
    #
    def initialize (self) :
        """ 
        initialize the shell connection.  We expect the pty_process to be in virgin
        state, i.e. to be newly forked and executed.  We thus expect shell
        startup prompts and messages.
        """

        self.prompt    = "^(.*[\$#>])\s*$" # greedy, look for line ending with # $ >
        self.prompt_re = re.compile (self.prompt, re.DOTALL)

        prompt_patterns = ["password\s*:\s*$",            # password prompt
                           "want to continue connecting", # hostkey confirmation
                           self.prompt]                   # native shell prompt 

        # self.prompt is all we need for local shell, so we could do:
        #
        # if  self.shell_type == 'sh' :
        #     prompt_patterns = [self.prompt] 
        #
        # but we don't and keep the other pattern around so that the switch in
        # the while loop below is the same for shell types


        # find a prompt
        n, match = self.pty_shell.find (prompt_patterns, _PTY_TIMEOUT)

        # this loop will run until we finally find the self.prompt.  At that
        # point, we'll try to set a different prompt, and when we found that,
        # too, we'll exit the loop and consider to be ready for running shell
        # commands.
        while True :

            if n == None :
                # we found none of the prompts, yet -- try again 
                n, match = self.pty_shell.find (prompt_patterns, _PTY_TIMEOUT)


            if n == 0 :
                self.logger.debug ("got password prompt")
                if not self.shell_pass :
                    raise saga.NoSuccess ("prompted for unknown password (%s)" \
                                       % match)

                self.pty_shell.write ("%s\n" % self.shell_pass)
                n, match = self.pty_shell.find (prompt_patterns, _PTY_TIMEOUT)


            elif n == 1 :
                self.logger.debug ("got hostkey prompt")
                self.pty_shell.write ("yes\n")
                n, match = self.pty_shell.find (prompt_patterns, _PTY_TIMEOUT)


            elif n == 2 :
                self.logger.debug ("got initial shell prompt")

                # turn off shell echo, set/register new prompt
                self.run_sync ("stty -echo; PS1='PROMPT-$?->\\n'; PS2=''; export PS1 PS2\n", 
                                new_prompt="PROMPT-(\d+)->\s*$")

                self.logger.debug ("got new shell prompt")

                # we are done waiting for a prompt
                break
        
        # check if some additional initialization routines as registered
        if  self.initialize_hook :
            self.initialize_hook ()

    # ----------------------------------------------------------------
    #
    def initialize_scp (self) :

        if self.scp :
            return




    # ----------------------------------------------------------------
    #
    def finalize (self, kill_pty = False) :

        try :
            # check if some additional initialization routines as registered
            if  self.finalize_hook :
                self.finalize_hook ()

        except Exception as e :
            pass


        try :
            if  kill_pty :
                if  self.pty_shell :
                    self.pty_shell.finalize ()


        except Exception as e :
            pass



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

        match = None

        while not match :
            _, match = self.pty_shell.find ([self.prompt], _PTY_TIMEOUT)

        ret, txt = self._eval_prompt (match)

        return (ret, txt)


    # ----------------------------------------------------------------
    #
    def set_prompt (self, prompt) :
        """
        :type  prompt:  string 
        :param prompt:  a regular expression matching the shell prompt

        The prompt regex is expected to be a regular expression with one set of
        catching brackets, which MUST return the previous command's exit status.
        This method will send a newline to the client, and expects to find the
        prompt with the exit value '0'.

        As a side effect, this method will discard all previous data on the pty,
        thus effectively flushing the pty output.  

        By encoding the exit value in the command prompt, we safe one roundtrip.
        The prompt on Posix compliant shells can be set, for example, via::

          PS1='PROMPT-$?->\\n'; export PS1

        The newline in the example above allows to nicely anchor the regular
        expression, which would look like::

          PROMPT-(\d+)->\s*$

        The regex is compiled with 're.DOTALL', so the dot character matches
        all characters, including line breaks.  Be careful not to match more
        than the exact prompt -- otherwise, a prompt search will swallow stdout
        data.  For example, the following regex::

          PROMPT-(.+)->\s*$

        would capture arbitrary strings, and would thus match *all* of::

          PROMPT-0-> ls
          data/ info
          PROMPT-0->

        and thus swallow the ls output...

        Note that the string match *before* te prompt regex is non-gready -- if
        the output contains multiple occurrences of the prompt, only the match
        up to the first occurence is returned.
        """

        old_prompt     = self.prompt
        self.prompt    = prompt
        self.prompt_re = re.compile ("^(.*?)%s\s*$" % self.prompt, re.DOTALL)

        try :
            self.pty_shell.write ("\n")

            # FIXME: how do we know that _PTY_TIMOUT suffices?  In particular if
            # we actually need to flush...
            _, match  = self.pty_shell.find ([self.prompt], _PTY_TIMEOUT)

            if not match :
                self.prompt = old_prompt
                raise saga.BadParameter ("Cannot use prompt, could not find it")

            ret, _ = self._eval_prompt (match)

            if  ret != 0 :
                self.prompt = old_prompt
                raise saga.BadParameter ("could not parse exit value (%s)" \
                                      % match)

        except Exception as e :
            self.prompt = old_prompt
            raise saga.NoSuccess ("Could not set prompt (%s)" % e)



    # ----------------------------------------------------------------
    #
    def _eval_prompt (self, data, new_prompt=None) :
        """
        This method will match the given data against the current prompt regex,
        and expects to find an integer as match -- which is then returned, along
        with all leading data, in a tuple
        """

        prompt    = self.prompt
        prompt_re = self.prompt_re

        if  new_prompt :
            prompt    = new_prompt
            prompt_re = re.compile ("^(.*)%s\s*$" % prompt, re.DOTALL)


        result = None
        try :
            if  not data :
                raise saga.NoSuccess ("could not parse prompt on empty string (%s) (%s)" \
                                   % (prompt, data))

            result = prompt_re.match (data)


            if  not result :
                self.logger.debug    ("could not parse prompt (%s) (%s)" % (prompt, data))
                raise saga.NoSuccess ("could not parse prompt (%s) (%s)" % (prompt, data))

            if  len (result.groups ()) != 2 :
                self.logger.debug    ("prompt does not capture exit value (%s)" % prompt)
                raise saga.NoSuccess ("prompt does not capture exit value (%s)" % prompt)

            txt =     result.group (1)
            ret = int(result.group (2)) 


        except Exception as e :
            self.logger.debug ("data   : %s" % data)
            self.logger.debug ("prompt : %s" % prompt)

            if  result and len(result.groups()) == 2 :
                self.logger.debug ("match 1: %s" % result.group (1))
                self.logger.debug ("match 2: %s" % result.group (2))

            raise saga.NoSuccess ("Could not eval prompt (%s)" % e)


        # if that worked, we can permanently set new_prompt
        if  new_prompt :
            self.set_prompt (new_prompt)

        return (ret, txt)



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

        command = command.strip ()
        if command.endswith ('&') :
            raise saga.BadParameter ("can only run foreground jobs ('%s')" \
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

        self.logger.debug ('run_sync: %s%s'   % (command, redir))
        self.pty_shell.write    (          "%s%s\n" % (command, redir))


        # If given, switch to new prompt pattern right now...
        prompt = self.prompt
        if  new_prompt :
            prompt = new_prompt

        # command has been started - now find prompt again.  
        _, match = self.pty_shell.find ([prompt], timeout=-1.0)  # blocks

        if not match :
            # not find prompt after blocking?  BAD!  Restart the shell
            self.finalize (kill_pty=True)
            raise saga.NoSuccess ("run_sync failed, no prompt (%s)" % command)


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
            _, match = self.pty_shell.find ([self.prompt], timeout=-1.0)  # blocks

            if not match :
                # not find prompt after blocking?  BAD!  Restart the shell
                self.finalize (kill_pty=True)
                raise saga.NoSuccess ("run_sync failed, no prompt (%s)" \
                                    % command)

            _ret, _stderr = self._eval_prompt (match)
            if  _ret :
                raise saga.NoSuccess ("run_sync failed, no stderr (%s: %s)" \
                                   % (_ret, _stderr))
            stderr =  _stderr


        if  iomode == STDOUT :
            stdout =  txt

        if  iomode == STDERR :
            stderr =  txt

        if  iomode == None :
            stdout =  txt


        return (ret, stdout, stderr)


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

        command = command.strip ()

        self.logger.debug ('run_async: %s'   % command)
        self.pty_shell.write    (           "%s\n" % command)

        return


    # ----------------------------------------------------------------
    #
    def stage_to_file (self, src, tgt) :
        """
        :type  src: string
        :param src: data to be staged into the target file

        :type  tgt: string
        :param tgt: path to file to be staged

        The content of the given string is pasted into a file (specified by tgt)
        on the remote system.  If that file exists, it is overwritten.
        A NoSuccess exception is raised if writing the file was not possible
        (missing permissions, incorrect path, etc.).

        See also :func:`stage_from_file`.
        """

        size_src = len (src)

        while True :

            ret, out, _ = self.run_sync ("wc -c %s.$$ | cut -f 1 -d ' '" % tgt)

            if  ret == 0 :
                size_tgt = out.strip ()

                if str(size_src) == str(size_tgt) :
                    break
                else :
                    # print "try again (%s)(%s)!" % (size_src, size_tgt)
                    pass


            if sys.platform == 'darwin' :
                self.pty_shell.write ("cat > %s.$$ <<'SAGA_ADAPTOR_SHELL_PTY_PROCESS_EOT'\n" % tgt)
                self.pty_shell.write (src)
                self.pty_shell.write ("\nSAGA_ADAPTOR_SHELL_PTY_PROCESS_EOT\n")

            else :
                src_hex = ""
                for i in [hex(ord(x)) for x in src] :
                    h = i.replace ('0x', '')
                    if len(h) == 1 : src_hex += '0'
                    src_hex += h
                
                self.pty_shell.write ("""\
                ( sed -e 's/\(..\)/\\1\\n/g' | 
                  while read A; do
                   if ! test -z "$A"; then printf "\\x$A"; fi
                  done ) > %s.$$ <<SAGA_ADAPTOR_SHELL_PTY_PROCESS_EOT\n""" % tgt)
                self.pty_shell.write (src_hex)
                self.pty_shell.write ("\nSAGA_ADAPTOR_SHELL_PTY_PROCESS_EOT\n")

            # we send two commands at once (cat, mv), so need to find two prompts
            ret, txt = self.find_prompt ()
            if  ret != 0 :
                raise saga.NoSuccess ("failed to stage (cat) string to file (%s)(%s)" % (ret, txt))


        self.pty_shell.write ("mv %s.$$ %s\n" % (tgt, tgt))  

        ret, txt = self.find_prompt ()
        if  ret != 0 :
            raise saga.NoSuccess ("failed to stage (mv) string to file (%s)(%s)" % (ret, txt))


    # ----------------------------------------------------------------
    #
    def stage_from_file (self, src) :
        """
        :type  src: string
        :param src: path to file to be fetched

        This is inverse to :func:`stage_to_file`: the content of a remote file
        specified by `src` will be returned as string.  A NoSuccess exception is
        raised if reading the file was not possible (missing permissions,
        incorrect path, etc.).  
        """

        ret, out, _ = self.run_sync ("cat %s" % src)

        if  ret != 0 :
            raise saga.NoSuccess ("failed to stage file to string (%s)(%s)" % (ret, out))

        return out


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

