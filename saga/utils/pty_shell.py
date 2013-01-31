
import re
import os

import saga.utils.pty_process
import saga.utils.logger

_PTY_TIMEOUT = 2.0
_SCHEMAS     = ['ssh', 'gsissh', 'fork']

IGNORE   = 0    # discard stdout / stderr
MERGED   = 1    # merge stdout and stderr
SEPARATE = 2    # fetch stdout and stderr individually (one more hop)
STDOUT   = 3    # fetch stdout only, discard stderr
STDERR   = 4    # fetch stderr only, discard stdout

###############################################################################
#
class pty_shell (object) :
    """
    This class wraps a shell process and runs it as a :class:`pty_process`.  The
    user of this class can start that shell, and run arbitrary commands on it.
    The shell to be run is expected to be POSIX compliant (bash, csh, sh, zsh
    etc).
    """

    def __init__ (self, url, contexts=[], logger=None) :

        self.url       = url               # describes the shell to run
        self.contexts  = contexts          # get security tokens from these
        self.logger    = logger            # possibly log to here
        self.prompt    = "^(.*[\$#>])\s*$" # a line ending with # $ >
        self.prompt_re = re.compile (self.prompt, re.DOTALL)
        
        # need a new logger?
        if not self.logger :
            self.logger = saga.utils.logger.getLogger ('pty_shell')

        schema  = self.url.schema.lower ()
        sh_type = ""
        sh_exe  = ""
        sh_pass = ""

        # find out what type of shell we have to deal with
        if  schema   == "ssh" :
            sh_type  =  "ssh"
            sh_exe   =  saga.utils.which.which ("ssh")

        elif schema  == "gsissh" :
            sh_type  =  "ssh"
            sh_exe   =  saga.utils.which.which ("gsissh")

        elif schema  == "fork" :

            sh_type  =  "sh"
            if  "SHELL" in os.environ :
                sh_exe =  saga.utils.which.which (os.environ["SHELL"])
            else :
                sh_exe =  saga.utils.which.which ("sh")
        else :
            raise saga.BadParameter._log (self.logger, \
            	  "pty_shell utility can only handle %s schema URLs, not %s" % (_SCHEMAS, schema))



        # make sure we have something to run
        if not sh_exe :
            raise saga.BadParameter._log (self.logger, \
            	  "SSH Job adaptor cannot handle the %s schema, no shell executable found" % schema)


        # depending on type, create pty_process command line (args, env etc)
        #
        # We always set term=vt100 to avoid ansi-escape sequences in the prompt
        # and elsewhere.  Also, we have to make sure that the shell is an
        # interactive login shell, so that it interprets the users startup
        # files, and reacts on commands.
        if  sh_type == "ssh" :

            sh_env  =  "/usr/bin/env TERM=vt100 "  # avoid ansi escapes
            sh_args =  "-t "                       # force pty
            sh_user =  ""                          # use default user as, well, default

            for context in self.contexts :

                if  context.type.lower () == "ssh" :
                    # ssh can handle user_id and user_key of ssh contexts
                    if  schema == "ssh" :
                        if  context.attribute_exists ("user_id") :
                            sh_user  = context.user_id
                        if  context.attribute_exists ("user_key") :
                            sh_args += "-i %s " % context.user_key

                if  context.type.lower () == "userpass" :
                    # FIXME: ssh should also be able to handle UserPass contexts
                    if  schema == "ssh" :
                        pass

                if  context.type.lower () == "gsissh" :
                    # gsissh can handle user_proxy of X509 contexts
                    # FIXME: also use cert_dir etc.
                    if  context.attribute_exists ("user_proxy") :
                        if  schema == "gsissh" :
                            sh_env = "X509_PROXY='%s' " % context.user_proxy

            # all ssh based shells allow for user_id from contexts -- but the
            # username given in the URL takes precedence
            if self.url.username :
                sh_user = self.url.username

            if sh_user :
                sh_args += "-l %s " % sh_user

            # build the ssh command line
            sh_cmd   =  "%s %s %s %s" % (sh_env, sh_exe, sh_args, self.url.host)


        # a local shell
        # Make sure we have an interactive login shell w/o ansi escapes.
        elif sh_type == "sh" :
            sh_args  =  "-l -i"
            sh_env   =  "/usr/bin/env TERM=vt100"
            sh_cmd   =  "%s %s %s" % (sh_env, sh_exe, sh_args)


        self.logger.info ("job service opens pty for '%s'" % sh_cmd)
        self.pty = saga.utils.pty_process.pty_process (sh_cmd, logger=self.logger)


        prompt_patterns = ["password\s*:\s*$",             # password prompt
                           "want to continue connecting",  # host key confirmation
                           self.prompt]                    # native shell prompt 
        # FIXME: consider to not do hostkey checks at all (see ssh options)

        if sh_type == 'sh' :
            # self.prompt is all we need for local shell, but we keep the
            # others around so that the switch in the while loop below is the
            # same for both shell types
            pass
            # prompt_patterns = [self.prompt] 


        # run the shell and find prompt
        (n, match) = self.pty.find (prompt_patterns, _PTY_TIMEOUT)

        # this loop will run until we finally find the self.prompt.  At that
        # point, we'll try to set a different prompt, and when we found that,
        # too, we'll exit the loop and consider to be ready for running shell
        # commands.
        while True :

            # we found none of the prompts, yet -- try again if the shell still
            # lives.
            if n == None :
                if not self.pty.alive () :
                    raise saga.NoSuccess ("failed to start shell\n- log ---\n%s\n---------\n" \
                                       % self.pty.get_cache_log ())

                # the write below will make our live simpler, as it will
                # eventually flush I/O buffers, and will make sure that we
                # get a decent prompt -- no matter what stupi^H^H^H^H^H nice
                # PS1 the user invented...
                #
                # FIXME: make sure this does not interfere with the host
                # key and password prompts.  For ssh's, a simple '\n' might
                # suffice...
              # self.pty.write ("export PS1='PROMPT-$?->\\n'\n")
              # self.pty.write ("\n")
                (n, match) = self.pty.find (prompt_patterns, _PTY_TIMEOUT)


            if n == 0 :
                self.pty.clog += "\n[pty_shell: got password prompt]\n"
                if not sh_pass :
                    raise saga.NoSuccess ("failed to start shell, prompted for unknown password\n- log ---\n%s\n---------\n" \
                                       % self.pty.get_cache_log ())

                self.pty.write ("%s\n" % sh_pass)
                (n, match) = self.pty.find (prompt_patterns, _PTY_TIMEOUT)


            elif n == 1 :
                self.pty.clog += "\n[pty_shell: got host key prompt]\n"
                self.pty.write ("yes\n")
                (n, match) = self.pty.find (prompt_patterns, _PTY_TIMEOUT)


            elif n == 2 :
                self.pty.clog += "\n[pty_shell: got initial shell prompt]\n"

                # try to set new prompt
                self.run_sync ("export PS1='PROMPT-$?->\\n'\n", new_prompt="PROMPT-(\d+)->\s*$")
                self.pty.clog += "\n[pty_shell: got new shell prompt]\n"

                # we are done waiting for a prompt
                break
        
        # we have a prompt on the remote system, and can now run commands.

        # FIXME: 
        self.clog = self.pty.clog


    def find_prompt (self) :

        (_, match)    = self.pty.find    ([self.prompt], _PTY_TIMEOUT)
        (txt, retval) = self.eval_prompt (match)

        return (txt, retval)


    def set_prompt (self, prompt) :
        """
        The prompt is expected to be a regular expression with one set of
        catching brackets, which MUST return the previous command's exit
        status.
        This method will send a newline to the client, and expects to find the
        prompt with the exit value '0'.

        As a side effect, this method will discard all previous data on the pty,
        thus effectively flushing the pty output.  
        """

        old_prompt     = self.prompt
        self.prompt    = prompt
        self.prompt_re = re.compile ("^(.*)%s\s*$" % self.prompt, re.DOTALL)

        try :
            self.pty.write ("true\n")

            # FIXME: how do we know that _PTY_TIMOUT suffices?  In particular if we
            # actually need to flush...
            (n, match)    = self.pty.find ([self.prompt], _PTY_TIMEOUT)
            (txt, retval) = self.eval_prompt (match)

            if not match :
                self.prompt = old_prompt
                raise saga.BadParameter ("Cannot use prompt, could not find it")

            if  retval != 0 :
                self.prompt = old_prompt
                raise saga.BadParameter ("Cannot use prompt, could not parse exit value (%s)" % match)

        except Exception as e :
            self.prompt = old_prompt
            raise saga.NoSuccess ("Could not set prompt (%s)" % e)



    def eval_prompt (self, data, new_prompt=None) :
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

        try :
            result = prompt_re.match (data)

            if  not result :
                raise saga.NoSuccess ("prompt is invalid, could not parse (%s) (%s)" % (prompt, data))

            if  len (result.groups ()) != 2 :
                raise saga.NoSuccess ("prompt is invalid, does not capture exit value (%s)" % prompt)

            text   =     result.group (1)
            retval = int(result.group (2)) 

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

        return (text, retval)






    def run_sync (self, command, iomode=MERGED, new_prompt=None) :

        # we expect the command to not to do stdio redirection, as
        # this is we want to capture that separately.  We *do* allow pipes 
        # and stdin/stdout redirection.  Note that SEPARATE mode will break if
        # the job is run in the background
        #
        # FIXME: document this!
        
        command = command.strip ()
        if command.endswith ('&') :
            raise saga.BadParameter ("run_sync can only run forground jobs, not '%s'" % command)

        redir  = ""
        errtmp = "/tmp/saga-python.ssh-job.stderr.$$"

        if  iomode == IGNORE :
            redir  =  " 1>>/dev/null 2>>/dev/null"

        if  iomode == MERGED :
            redir  =  " 2>&1"

        if  iomode == SEPARATE :
            redir  =  " 2>%s" % errtmp

        if  iomode == STDOUT :
            redir  =  " 2>/dev/null"

        if  iomode == STDERR :
            redir  =  " 2>&1 1>/dev/null"

        prompt = self.prompt
        if  new_prompt :
            prompt = new_prompt

        self.pty.write ("%s%s\n" % (command, redir))
        _, match = self.pty.find ([prompt], timeout=-1.0)  # this blocks forever

        if not match :
            # not find prompt after blocking?  BAD!  restart the shell
            self.close ()
            raise saga.NoSuccess ("run_sync failed, could not find prompt again (%s)" % command)


        (txt, retval) = self.eval_prompt (match, new_prompt)

        stdout = None
        stderr = None

        if  iomode == IGNORE :
            pass

        if  iomode == MERGED :
            stdout =  txt

        if  iomode == SEPARATE :
            stdout =  txt

            self.pty.write ("cat %s\n" % errtmp)
            _, match = self.pty.find ([self.prompt], timeout=-1.0)  # this blocks forever

            if not match :
                # not find prompt after blocking?  BAD!  restart the shell
                self.close ()
                raise saga.NoSuccess ("run_sync failed, could not find prompt after stderr fetching (%s)" % command)

            (stderrtmp, retvaltmp) = self.eval_prompt (match)
            if  retvaltmp :
                raise saga.NoSuccess ("run_sync failed, could fetch stderr (%s: %s)" % (retval, stderrtmp))

            stderr =  stderrtmp


        if  iomode == STDOUT :
            stdout =  txt

        if  iomode == STDERR :
            stderr =  txt


        return (retval, stdout, stderr)


    def __del__ (self) :

        try :
            if self.pty : 
                del (self.pty)
        except :
            pass


    def find (self, patterns, timeout=0) :
        return self.pty.find (patterns, timeout)

    def write (self, data) :
        return self.pty.write (data)

    def get_cache_log (self) :
        return self.pty.get_cache_log ()

     


    # ----------------------------------------------------------------
    #
    #
    def _run_job (self, jd) :
        """ runs a job on the wrapper via pty, and returns the job id """

        exe = jd.executable
        arg = ""
        env = ""
        cwd = ""

        if  not self.pty :
            raise saga.IncorrectState ("job service is not connected to backend")

        if jd.attribute_exists ("arguments") :
            for a in jd.arguments :
                arg += " %s" % a

        if jd.attribute_exists ("environment") :
            env = "/usr/bin/env"
            for e in jd.environment :
                env += " %s=%s"  %  (e, jd.environment[e])
            env += " "

        if jd.attribute_exists ("working_directory") :
            cwd = "cd %s && " % jd.working_directory

        cmd = "%s %s %s %s"  %  (env, cwd, exe, arg)

        # make sure we did not yet get a timeout
        _, match = self.pty.find (["^IDLE TIMEOUT"])
        if match :
            # shell timed out, need to restart
            self.pty.clog += "\n~~~~~ got timeout\n"
            self.logger.info ("restarting remote shell wrapper")

            self.pty.write ("/bin/sh $HOME/.saga/adaptors/ssh_job/wrapper.sh\n")
            _, match = self.pty.find  (["PROMPT"], _PTY_TIMEOUT)

            if  not match or not match[-3:] == "PROMPT" :
                raise saga.NoSuccess ("failed to run job service wrapper\n- log ---\n%s\n---------\n" \
                                   % self.pty.get_cache_log ())

            self.pty.clog += "\n~~~~~ got cmd prompt\n"
        else :
            # self.pty.clog += "\n~~~~~ no timeout\n"
            pass
        
        self.pty.write ("RUN %s\n" % cmd)
        _, match = self.pty.find (["PROMPT"], _PTY_TIMEOUT)

        if  not match :
            raise saga.NoSuccess ("failed to run job -- backend error\n- log ---\n%s\n---------\n" \
                               % self.pty.get_cache_log ())
        if  not match[-3:] == "PROMPT" :
            raise saga.NoSuccess ("failed to run job -- backend error (%s)\n- log ---\n%s\n---------\n" \
                               % (match, self.pty.get_cache_log ()))


        splitlines = match.split ("\n")
        lines = []

        for line in splitlines :
            if len (line) :
                lines.append (line)
        self.logger.debug (str(lines))

        if len (lines) < 3 :
            raise saga.NoSuccess ("failed to run job\n- log ---\n%s\n---------\n" \
                               % "\n".join (lines))

        # FIXME: we should know which line says ok...
        ok = False
        for line in lines :
            if line == "OK" :
                ok = True
                break

        if not ok :
            self.logger.warn ("Did not find 'OK' from wrapper.sh (%s)" % str (lines))
            raise saga.NoSuccess ("failed to run job\n- log ---\n%s\n---------\n" \
                               % "\n".join (lines))

        job_id = "[%s]-[%s]" % (self.url, lines[2])

        self.logger.debug ("started job %s" % job_id)

        return job_id
        


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

