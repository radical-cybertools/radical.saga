
import re
import os
import sys
import pty
import time
import shlex
import select
import signal
import threading

import saga.utils.logger
import saga.utils.timeout_gc
import saga.exceptions as se

# --------------------------------------------------------------------
#
_CHUNKSIZE = 1024  # default size of each read
_POLLDELAY = 0.01  # seconds in between read attempts
_DEBUG_MAX = 600


# --------------------------------------------------------------------
#
class PTYProcess (object) :
    """
    This class spawns a process, providing that child with pty I/O channels --
    it will maintain stdin, stdout and stderr channels to the child.  All
    write-like operations operate on the stdin, all read-like operations operate
    on the stdout stream.  Data from the stderr stream are at this point
    redirected to the stdout channel.

    Example::

        # run an interactive client process
        pty = PTYProcess ("/usr/bin/ssh -t localhost")

        # check client's I/O for one of the following patterns (prompts).  
        # Then search again.
        n, match = pty.find (['password\s*:\s*$', 
                              'want to continue connecting.*\(yes/no\)\s*$', 
                              '[\$#>]\s*$'])

        while True :

            if n == 0 :
                # found password prompt - tell the secret
                pty.write ("secret\\n")
                n, _ = pty.find (['password\s*:\s*$', 
                                  'want to continue connecting.*\(yes/no\)\s*$', 
                                  '[\$#>]\s*$'])
            elif n == 1 :
                # found request to accept host key - sure we do... (who checks
                # those keys anyways...?).  Then search again.
                pty.write ("yes\\n")
                n, _ = pty.find (['password\s*:\s*$', 
                                  'want to continue connecting.*\(yes/no\)\s*$', 
                                  '[\$#>]\s*$'])
            elif n == 2 :
                # found shell prompt!  Wohoo!
                break
        

        while True :
            # go full Dornroeschen (Sleeping Beauty)...
            pty.alive (recover=True) or break      # check / restart process
            pty.find  (['[\$#>]\s*$'])             # find shell prompt
            pty.write ("/bin/sleep "100 years"\\n") # sleep!  SLEEEP!

        # something bad happened
        print pty.autopsy ()


    The managed child process is under control of a Timeout Garbage Collector
    (:class:`saga.utils.timeout_gc.TimeoutGC`), which will terminate the child
    after some inactivity period.  The child will be automatically restarted on
    the next activity attempts.  To support orderly process bootstrapping, users
    of the :class:`PTYProcess` class should register hooks for process
    initialization and finalization (:func:`set_initialize_hook` and
    :func:`set_finalize_hook`).  The finalization hook may operate on a dead
    child process, and should be written in a way that this does not lead to an
    error (which would abort the restart attempt).

    If the child process dies on its own, or is terminated by a third party, the
    class will also attempt to restart the child.  In order to not interfere
    with the process state at unexpected points, this will only happen during
    explicit :func:`alive` checks, if the `recover` parameter is set to `True`
    (`False` by default).  This restart mechanism will be used up to
    `recover_max` times in a row, any successful activity will reset the recover
    counter though.  The recover process will invoke both the finalization and
    initialization hooks.
    """

    # ----------------------------------------------------------------
    #
    def __init__ (self, command, logger=None) :
        """
        The class constructor, which runs (execvpe) command in a separately
        forked process.  The bew process will inherit the environment of the
        application process.

        :type  command: string or list of strings
        :param command: The given command is what is run as a child, and
        fed/drained via pty pipes.  If given as string, command is split into an
        array of strings, using :func:`shlex.split`.

        :type  logger:  :class:`saga.utils.logger.Logger` instance
        :param logger:  logger stream to send status messages to.
        """

        if isinstance (command, basestring) :
            command = shlex.split (command)

        if not isinstance (command, list) :
            raise se.BadParameter ("PTYProcess expects string or list command")

        if len(command) < 1 :
            raise se.BadParameter ("PTYProcess expects non-empty command")


        self.command = command # list of strings too run()
        self.logger  = logger


        self.cache   = ""      # data cache
        self.child   = None    # the process as created by subprocess.Popen
        self.ptyio   = None    # the process' io channel, from pty.fork()

        self.exit_code        = None  # child died with code (may be revived)
        self.exit_signal      = None  # child kill by signal (may be revived)

        self.initialize_hook  = None
        self.finalize_hook    = None

        self.recover_max      = 3  # TODO: make configure option.  This does not
        self.recover_attempts = 0  # apply for recovers triggered by gc_timeout!

        if not self.logger :
            self.logger = saga.utils.logger.getLogger ('PTYProcess')

        # register this process instance for timeout garbage collection
        self.gc = saga.utils.timeout_gc.TimeoutGC ()
        self.gc.register (self, self.initialize, self.finalize)


        try :
            self.initialize ()

        except Exception as e :
            raise se.NoSuccess ("pty or process creation failed (%s)" % e)

    # --------------------------------------------------------------------
    #
    def __del__ (self) :
        """ 
        Need to free pty's on destruction, otherwise we might ran out of
        them (see cat /proc/sys/kernel/pty/max)
        """
    
        self.logger.error ("pty __del__")
      # self.logger.trace ()
    
        try :
            self.gc.unregister (self)
            self.finalize ()
        except :
            pass
    

    # ----------------------------------------------------------------------
    #
    def set_initialize_hook (self, initialize_hook) :
        self.initialize_hook = initialize_hook

    # ----------------------------------------------------------------------
    #
    def set_finalize_hook (self, finalize_hook) :
        self.finalize_hook = finalize_hook

    # ----------------------------------------------------------------------
    #
    def initialize (self) :

        # NOTE: do we need to lock?

        self.logger.debug ("PTYProcess: '%s'" % ' '.join ((self.command)))

        self.parent_in,  self.child_in  = pty.openpty ()
        self.parent_out, self.child_out = pty.openpty ()
      # self.parent_err, self.child_err = pty.openpty ()

        self.parent_io,  self.child_io  = pty.openpty ()

        # create the child
        try :
             self.child =  os.fork ()
        except Exception as e:
            raise se.NoSuccess ("Could not run (%s): %s" \
                             % (' '.join (self.command), e))
        
        if  not self.child :
            # this is the child

            try :
                # close parent end of pty pipes
                os.close (self.parent_in)
                os.close (self.parent_out)
              # os.close (self.parent_err)

                # reopen child stdio unbuffered (buffsize=0)
                unbuf_in  = os.fdopen (sys.stdin.fileno  (), 'r+', 0)
                unbuf_out = os.fdopen (sys.stdout.fileno (), 'w+', 0)
                unbuf_err = os.fdopen (sys.stderr.fileno (), 'w+', 0)
               
                # redirect our precious stdio
                os.dup2 (self.child_in,  unbuf_in.fileno  ())
                os.dup2 (self.child_out, unbuf_out.fileno ())
                os.dup2 (self.child_out, unbuf_err.fileno ())
              # os.dup2 (self.child_err, unbuf_err.fileno ())

                # make a process group leader (should close tty tty)
                os.setsid ()

                # close tty, in case we still own any:
                try :
                    os.close (os.open ("/dev/tty", os.O_RDWR | os.O_NOCTTY));
                except :
                    # was probably closed earlier, that's all right
                    pass

                # now acquire pty
                try :
                    os.close (os.open (os.ttyname (sys.stdout.fileno ()), os.O_RDWR))
                except :
                    # well, this *may* be bad - or may now, depending on the
                    # type of command ones to run in this shell.  So, we print
                    # a big fat warning, and continue
                    self.logger.error ("Unclean PTY shell setup - proceed anyway")
                    pass

                # all I/O set up, have a pty (*fingers crossed*), lift-off!
                os.execvpe (self.command[0], self.command, os.environ)

            except OSError as e:
                self.logger.error ("Could not execute (%s): %s" \
                                % (' '.join (self.command), e))
                sys.exit (-1)

        else :
            # parent
            os.close (self.child_in)
            os.close (self.child_out)
          # os.close (self.child_err)


        # check if some additional initialization routines as registered
        if  self.initialize_hook :
            self.initialize_hook ()


    # --------------------------------------------------------------------
    #
    def finalize (self) :
        """ kill the child, close all I/O channels """

        # NOTE: do we need to lock?

        # as long as the chiuld lives, run any higher level shutdown routine.
        if  self.finalize_hook :
            self.finalize_hook ()

        # now we can safely kill the child process, and close all I/O channels
        try :
            if  self.child :
                os.kill (self.child, signal.SIGTERM)
        except OSError :
            pass

        try :
            if  self.child :
                os.kill (self.child, signal.SIGKILL)
        except OSError :
            pass

        self.child = None

     ## try : 
     ##     os.close (self.parent_in)  
     ## except OSError :
     ##     pass

     ## try : 
     ##     os.close (self.parent_out) 
     ## except OSError :
     ##     pass

      # try : 
      #     os.close (self.parent_err) 
      # except OSError :
      #     pass


    # --------------------------------------------------------------------
    #
    def wait (self) :
        """ 
        blocks forever until the child finishes on its own, or is getting
        killed
        """

        with self.gc.active (self) :

            # yes, for ever and ever...
            while True :

                # hey, kiddo, whats up?
                wpid, wstat = os.waitpid (self.child, 0)

                # did we get a note about child termination?
                if 0 == wpid :

                    # nope, all is well - carry on
                    continue


                # Yes, we got a note.  
                # Well, maybe the child fooled us and is just playing dead?
                if os.WIFSTOPPED   (wstat) or \
                   os.WIFCONTINUED (wstat)    :
                    # we don't care if someone stopped/resumed the child -- that is up
                    # to higher powers.  For our purposes, the child is alive.  Ha!
                    continue


                # not stopped, poor thing... - soooo, what happened??
                if os.WIFEXITED (wstat) :
                    # child died of natural causes - perform autopsy...
                    self.exit_code   = os.WEXITSTATUS (wstat)
                    self.exit_signal = None

                elif os.WIFSIGNALED (wstat) :
                    # murder!! Child got killed by someone!  recover evidence...
                    self.exit_code   = None
                    self.exit_signal = os.WTERMSIG (wstat)

                # either way, its dead -- make sure it stays dead, to avoid zombie
                # apocalypse...
                self.finalize ()
                return

    # --------------------------------------------------------------------
    #
    def alive (self, recover=False) :
        """
        try to determine if the child process is still active.  If not, mark 
        the child as dead and close all IO descriptors etc ("func:`finalize`).

        If `recover` is `True` and the child is indeed dead, we attempt to
        re-initialize it (:func:`initialize`).  We only do that for so many
        times (`self.recover_max`) before giving up -- at that point it seems
        likely that the child exits due to a re-occurring operations condition.

        Note that upstream consumers of the :class:`PTYProcess` should be
        careful to only use `recover=True` when they can indeed handle
        a disconnected/reconnected client at that point, i.e. if there are no
        assumptions on persistent state beyond those in control of the upstream
        consumers themselves.
        """

        with self.gc.active (self) :

            # do we have a child which we can check?
            if  self.child :

                # hey, kiddo, whats up?
                wpid, wstat = os.waitpid (self.child, os.WNOHANG)

                # did we get a note about child termination?
                if 0 == wpid :
                    # nope, all is well - carry on
                    return True


                # Yes, we got a note.  
                # Well, maybe the child fooled us and is just playing dead?
                if os.WIFSTOPPED   (wstat) or \
                   os.WIFCONTINUED (wstat)    :
                    # we don't care if someone stopped/resumed the child -- that is up
                    # to higher powers.  For our purposes, the child is alive.  Ha!
                    return True


                # not stopped, poor thing... - soooo, what happened??
                if os.WIFEXITED (wstat) :
                    # child died of natural causes - perform autopsy...
                    self.exit_code   = os.WEXITSTATUS (wstat)
                    self.exit_signal = None

                elif os.WIFSIGNALED (wstat) :
                    # murder!! Child got killed by someone!  recover evidence...
                    self.exit_code   = None
                    self.exit_signal = os.WTERMSIG (wstat)

                # either way, its dead -- make sure it stays dead, to avoid zombie
                # apocalypse...
                self.finalize ()

            # check if we can attempt a post-mortem revival though
            if  not recover :
                # nope, we are on holy ground - revival not allowed.
                return False

            # we are allowed to revive!  So can we try one more time...  pleeeease??
            # (for cats, allow up to 9 attempts; for Buddhists, always allow to
            # reincarnate, etc.)
            if self.recover_attempts >= self.recover_max :
                # nope, its gone for good - just report the sad news
                return False

            # MEDIIIIC!!!!
            self.recover_attempts += 1
            self.initialize ()

            # well, now we don't trust the child anymore, of course!  So we check
            # again.  Yes, this is recursive -- but note that recover_attempts get
            # incremented on every iteration, and this will eventually lead to
            # call termination (tm).
            return self.alive (recover=True)



    # --------------------------------------------------------------------
    #
    def autopsy (self) :
        """ 
        return diagnostics information string for dead child processes
        """

        with self.gc.active (self) :

            if  self.child :
                # Boooh!
                return "false alarm, process %s is alive!" % self.child

            ret  = ""
            ret += "  exit code  : %s\n" % self.exit_code
            ret += "  exit signal: %s\n" % self.exit_signal
            ret += "  last output: %s\n" % self.cache[-256:] # FIXME: smarter selection

            return ret


    # --------------------------------------------------------------------
    #
    def read (self, size=0, timeout=0, _force=False) :
        """ 
        read some data from the child.  By default, the method reads whatever is
        available on the next read, up to _CHUNKSIZE, but other read sizes can
        be specified.  
        
        The method will return whatever data it has at timeout::
        
          timeout == 0 : return the content of the first successful read, with
                         whatever data up to 'size' have been found.
          timeout <  0 : return after first read attempt, even if no data have 
                         been available.

        If no data are found, the method returns an empty string (not None).

        This method will not fill the cache, but will just read whatever data it
        needs (FIXME).

        Note: the returned lines do *not* get '\\\\r' stripped.
        """

        with self.gc.active (self) :

            if not self.alive (recover=False) :
                raise se.NoSuccess ("cannot read from dead pty process (%s)" \
                                 % self.cache[-256:])


            try:
                # start the timeout timer right now.  Note that even if timeout is
                # short, and child.poll is slow, we will nevertheless attempt at least
                # one read...
                start = time.time ()

                # read until we have enough data, or hit timeout ceiling...
                while True :
                
                    # first, lets see if we still have data in the cache we can return
                    if len (self.cache) :

                        if not size :
                            ret = self.cache
                            self.cache = ""
                            return ret

                        # we don't even need all of the cache
                        elif size <= len (self.cache) :
                            ret = self.cache[:size]
                            self.cache = self.cache[size:]
                            return ret

                    # otherwise we need to read some more data, right?
                    # idle wait 'til the next data chunk arrives, or 'til _POLLDELAY
                    rlist, _, _ = select.select ([self.parent_out], [], [], _POLLDELAY)

                    # got some data? 
                    for f in rlist:
                        # read whatever we still need

                        readsize = _CHUNKSIZE
                        if size: 
                            readsize = size-len(ret)

                        buf  = os.read (f, _CHUNKSIZE)

                        if  len(buf) == 0 and sys.platform == 'darwin' :
                            self.logger.debug ("read : MacOS EOF")
                            self.finalize ()
                            raise se.NoSuccess ("process on MacOS died (%s)" \
                                             % self.cache[-256:])


                        self.cache += buf.replace ('\r', '')
                        log         = buf.replace ('\r', '')
                        log         = log.replace ('\n', '\\n')
                      # print "buf: --%s--" % buf
                      # print "log: --%s--" % log
                        if  len(log) > _DEBUG_MAX :
                            self.logger.debug ("read : [%5d] (%s ... %s)" \
                                            % (len(log), log[:30], log[-30:]))
                        else :
                            self.logger.debug ("read : [%5d] (%s)" \
                                            % (len(log), log))


                    # lets see if we still got any data in the cache we can return
                    if len (self.cache) :

                        if not size :
                            ret = self.cache
                            self.cache = ""
                            return ret

                        # we don't even need all of the cache
                        elif size <= len (self.cache) :
                            ret = self.cache[:size]
                            self.cache = self.cache[size:]
                            return ret

                    # at this point, we do not have sufficient data -- only
                    # return on timeout

                    if  timeout == 0 : 
                        # only return if we have data
                        if len (self.cache) :
                            ret        = self.cache
                            self.cache = ""
                            return ret

                    elif timeout < 0 :
                        # return of we have data or not
                        ret        = self.cache
                        self.cache = ""
                        return ret

                    else : # timeout > 0
                        # return if timeout is reached
                        now = time.time ()
                        if (now-start) > timeout :
                            ret        = self.cache
                            self.cache = ""
                            return ret


            except Exception as e :
                raise se.NoSuccess ("read from pty process [%s] failed (%s)" \
                                 % (threading.current_thread().name, e))


    # ----------------------------------------------------------------
    #
    def find (self, patterns, timeout=0) :
        """
        This methods reads bytes from the child process until a string matching
        any of the given patterns is found.  If that is found, all read data are
        returned as a string, up to (and including) the match.  Note that
        pattern can match an empty string, and the call then will return just
        that, an empty string.  If all patterns end with matching a newline,
        this method is effectively matching lines -- but note that '$' will also
        match the end of the (currently available) data stream.

        The call actually returns a tuple, containing the index of the matching
        pattern, and the string up to the match as described above.

        If no pattern is found before timeout, the call returns (None, None).
        Negative timeouts will block until a match is found

        Note that the pattern are interpreted with the re.M (multi-line) and
        re.S (dot matches all) regex flags.

        Performance: the call is doing repeated string regex searches over
        whatever data it finds.  On complex regexes, and large data, and small
        read buffers, this method can be expensive.  

        Note: the returned data get '\\\\r' stripped.
        """

        try :
            start = time.time ()                       # startup timestamp
            ret   = []                                 # array of read lines
            patts = []                                 # compiled patterns
            data  = self.cache                         # initial data to check
            self.cache = ""

            if not data : # empty cache?
                data = self.read (timeout=_POLLDELAY)

            # pre-compile the given pattern, to speed up matching
            for pattern in patterns :
                patts.append (re.compile (pattern, re.MULTILINE | re.DOTALL))

            # we wait forever -- there are two ways out though: data matches
            # a pattern, or timeout passes
            while True :

              # time.sleep (0.1)

                # skip non-lines
                if  None == data :
                    data += self.read (timeout=_POLLDELAY)

                # check current data for any matching pattern
              # print ">>%s<<" % data
                for n in range (0, len(patts)) :

                    match = patts[n].search (data)
                  # print "==%s==" % patterns[n]

                    if match :
                        # a pattern matched the current data: return a tuple of
                        # pattern index and matching data.  The remainder of the
                        # data is cached.
                        ret  = data[0:match.end()]
                        self.cache = data[match.end():] 

                      # print "~~match!~~ %s" % data[match.start():match.end()]
                      # print "~~match!~~ %s" % (len(data))
                      # print "~~match!~~ %s" % (str(match.span()))
                      # print "~~match!~~ %s" % (ret)

                        return (n, ret.replace('\r', ''))

                # if a timeout is given, and actually passed, return a non-match
                if timeout == 0 :
                    return (None, None)

                if timeout > 0 :
                    now = time.time ()
                    if (now-start) > timeout :
                        self.cache = data
                        return (None, None)

                # no match yet, still time -- read more data
                data += self.read (timeout=_POLLDELAY)


        except Exception as e :
            raise se.NoSuccess ("find from pty process [%s] failed (%s) (%s)" \
                             % (threading.current_thread().name, e, data))



    # ----------------------------------------------------------------
    #
    def write (self, data) :
        """
        This method will repeatedly attempt to push the given data into the
        child's stdin pipe, until it succeeds to write all data.
        """

        with self.gc.active (self) :
        
            if not self.alive (recover=False) :
                raise se.NoSuccess ("cannot write to dead pty process (%s)" \
                                 % self.cache[-256:])

            try :

                log = data.replace ('\n', '\\n')
                log =  log.replace ('\r', '')
                if  len(log) > _DEBUG_MAX :
                    self.logger.debug ("write: [%5d] (%s ... %s)" \
                                    % (len(data), log[:30], log[-30:]))
                else :
                    self.logger.debug ("write: [%5d] (%s)" \
                                    % (len(data), log))

                # attempt to write forever -- until we succeeed
                while data :

                    # check if the pty pipe is ready for data
                    _, wlist, _ = select.select ([], [self.parent_in], [], _POLLDELAY)

                    for f in wlist :
                        
                        # write will report the number of written bytes
                        size = os.write (f, data)

                        # otherwise, truncate by written data, and try again
                        data = data[size:]

                        if data :
                            self.logger.info ("write: [%5d]" % size)


            except Exception as e :
                raise se.NoSuccess ("write to pty process [%s] failed (%s)" \
                                 % (threading.current_thread().name, e))



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

