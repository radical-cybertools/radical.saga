
import re
import os
import sys
import pty
import time
import shlex
import select
import signal

import saga.utils.logger
import saga.exceptions as se


# --------------------------------------------------------------------
#
_CHUNKSIZE = 1024   # default size of each read
_POLLDELAY = 0.0001 # seconds in between read attempts


# --------------------------------------------------------------------
#
class PTYProcess (object) :
    """
    This class spawns a process, providing that child with pty io channels --
    it will maintain stdin, stdout and stderr channels to the child.  All
    write* operations operate on the stdin, all read* operations operate on the
    stdout stream.  Data from the stderr stream are at this point redirected to
    the stdout channel.

    Example::

        pty = PTYProcess ("/usr/bin/ssh -t localhost")
        pty.run ()

        n, match = pty.find (['password\s*:\s*$', 
                              'want to continue connecting.*\(yes/no\)\s*$', 
                              '[\$#>]\s*$'])

        while True :

            if n == 0 :
                # found password prompt
                pty.write ("secret\\n")
                n, match = pty.find (['password\s*:\s*$', 
                                      'want to continue connecting.*\(yes/no\)\s*$', 
                                      '[\$#>]\s*$'])
            elif n == 1 :
                # found request to accept host key
                pty.write ("yes\\n")
                n, match = pty.find (['password\s*:\s*$', 
                                      'want to continue connecting.*\(yes/no\)\s*$', 
                                      '[\$#>]\s*$'])
            elif n == 2 :
                # found some prompt
                break
        
        while pty.alive () :
            # send sleeps as quickly as possible, forever...
            pty.find (['[\$#>]\s*$'])
            pty.write ("/bin/sleep 10\\n")
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
        array of strings (simple splis on white space), as that is what
        :func:`subprocess.Popen` wants.

        :type  command: string or list of strings
        :param command: The given command is what is run as a child, and
        """

        if isinstance (command, basestring) :
            command = shlex.split (command)

        if not isinstance (command, list) :
            raise se.BadParameter ("PTYProcess expects string or list command")

        if len(command) < 1 :
            raise se.BadParameter ("PTYProcess expects non-empty command")


        try :

            self.command = command # list of strings too run()
            self.cache   = ""      # data cache
            self.clog    = ""      # log the data cache
            self.child   = None    # the process as created by subprocess.Popen
            self.logger  = logger

            if not self.logger :
                self.logger = saga.utils.logger.getLogger ('PTYProcess')

            self.logger.debug ("PTYProcess: %s\n\n" % ' '.join ((command)))

            self.parent_in,  self.child_in  = pty.openpty ()
            self.parent_out, self.child_out = pty.openpty ()
         #  self.parent_err, self.child_err = pty.openpty ()

            # create the child
            try :
                self.child = os.fork ()
            except Exception as e:
                raise se.NoSuccess ("Could not run (%s): %s" \
                                 % (' '.join (command), e))
            
            if not self.child :
                try :
                    # this is the child
                    os.close (self.parent_in)
                    os.close (self.parent_out)
                  # os.close (self.parent_err)
                    
                  # # reopen stdio unbuffered
                  # # 
                  # # this mechanism is actually useful, but, for some obscure
                  # # (to me) reason fails badly if the applications stdio is
                  # # redirected -- which is a very valid use case.  So, we
                  # # keep I/O buffered, and need to get pipes flushed otherwise
                  # # (newlines much?)
                  # unbuf_in  = os.fdopen (sys.stdin.fileno(),  'r+', 0)
                  # unbuf_out = os.fdopen (sys.stdout.fileno(), 'w+', 0)
                  # unbuf_err = os.fdopen (sys.stderr.fileno(), 'w+', 0)
                  #
                  # os.dup2 (self.child_in,  unbuf_in.fileno())
                  # os.dup2 (self.child_out, unbuf_out.fileno())
                  # os.dup2 (self.child_out, unbuf_err.fileno())

                    # redirect stdio
                    os.dup2 (self.child_in,  sys.stdin.fileno())
                    os.dup2 (self.child_out, sys.stdout.fileno())
                    os.dup2 (self.child_out, sys.stderr.fileno())

                    os.execvpe (self.command[0], self.command, os.environ)

                except OSError as e:
                    self.logger.error ("Could not execute (%s): %s" \
                                    % (' '.join (command), e))
                    sys.exit (-1)

            else :
                # parent
                os.close (self.child_in)
                os.close (self.child_out)
              # os.close (self.child_err)


            if not self.alive () :
                raise se.NoSuccess ("Could not run (%s)" % ' '.join (command))


        except Exception as e :
            raise se.NoSuccess ("pty or process creation failed (%s)" % e)

    # --------------------------------------------------------------------
    #
    def __del__ (self) :
        """ 
        Need to free pty's on destruction, otherwise we might ran out of
        them (see cat /proc/sys/kernel/pty/max)
        """

        self.logger.debug ("__del__")

        self.close ()


    # --------------------------------------------------------------------
    #
    def close (self) :
        """ kill the child, close all I/O channels """

        try :
            if  self.alive () :
                os.kill (self.child, signal.SIGTERM)
        except OSError :
            pass

        try :
            if  self.alive () :
                os.kill (self.child, signal.SIGKILL)
        except OSError :
            pass

        self.child = None

        try : 
            os.close (self.parent_in)  
        except OSError :
            pass

        try : 
            os.close (self.child_in)    
        except OSError :
            pass

        try : 
            os.close (self.parent_out) 
        except OSError :
            pass

        try : 
            os.close (self.child_out)   
        except OSError :
            pass

      # try : 
      #     os.close (self.parent_err) 
      # except OSError :
      #     pass
      #
      # try : 
      #     os.close (self.child_err)   
      # except OSError :
      #     pass


    # --------------------------------------------------------------------
    #
    def alive (self) :
        """
        alive() checks if the child gave an exit value -- if none, it is assumed
        to be still alive.
        """

        if not self.child :
            return False

        try :
            pid, status = os.waitpid (self.child, os.WNOHANG)

            if (pid, status) == (0, 0) :
                return True

        except OSError as e :
            self.logger.debug ("cannot check child status (%s)" % e)


        # child is dead -- get last data into clog, if any
        try :
            self.read (timeout=0.01, _force=True)
        except OSError :
            pass
        except se.SagaException :
            pass

        # avoid any further activity, inclusive waitpid
        self.child = None

        return False


    # --------------------------------------------------------------------
    #
    def read (self, size=_CHUNKSIZE, timeout=0, _force=False) :
        """ 
        read some data from the child.  By default, the method reads a full
        chunk, but other read sizes can be specified.  
        
        The method will return whatever data is has at timeout::
        
          timeout == 0 : return the content of the first successful read, with
                         whatever data up to 'size' have been found.
          timeout <  0 : return after first read attempt, even if no data have 
                         been available.

        If no data are found, the method returns an empty string (not None).

        This method will not fill the cache, but will just read whatever data it
        needs (FIXME).

        Note: the returned lines do *not* get '\\\\r' stripped.
        """

        # start the timeout timer right now.  Note that even if timeout is
        # short, and child.poll is slow, we will nevertheless attempt at least
        # one read...
        start = time.time ()

        if not _force :
            if not self.alive () :
                raise se.NoSuccess ("Could not read - pty process died")

        ret     = ""
        sel_to  = timeout

        # the select timeout cannot be negative -- 0 is non-blocking... 
        if  sel_to < 0 : 
            sel_to = 0


        # first, lets see if we still have data in the cache we can return
        if len (self.cache) :

            # we don't even need all of the cache
            if size < len (self.cache) :
                ret = self.cache[:size]
                self.cache = self.cache[size:]
                return ret

            elif size == len (self.cache) :
                # doh!
                ret = self.cache
                self.cache = ""
                return ret

            else : # size > len(self.cache)
                # just use what we have, then go on to reading data
                ret = self.cache
                self.cache = ""


        try:
            # read until we have enough data, or hit timeout ceiling...
            while True :
            
                # idle wait 'til the next data chunk arrives, or 'til sel_to
                rlist, _, _ = select.select ([self.parent_out], [], [], sel_to)

                # got some data? 
                for f in rlist:
                    # read whatever we still need
                    buf  = os.read (f, size-len(ret))
                    self.clog += buf
                    ret       += buf

                    buf = buf.replace ('\n', '\\n')
                    buf = buf.replace ('\r', '')
                    if  len(buf) > 40 :
                        self.logger.debug ("read : [%5d] (%s ... %s)" \
                                        % (len(buf), buf[:20], buf[-20:]))
                    else :
                        self.logger.debug ("read : [%5d] (%s)" \
                                        % (len(buf), buf))

                if timeout == 0 : 
                    # only return if we have data
                    if len (ret) :
                        return ret

                elif timeout < 0 :
                    # return immediately
                    return ret

                else : # timeout > 0
                    # return if timeout is reached, or if data size is reached
                    if len (ret) >= size :
                        return ret

                    now = time.time ()
                    if (now-start) > timeout :
                        return ret

        except Exception as e :
            raise se.NoSuccess ("read from pty process failed (%s)" % e)



    # --------------------------------------------------------------------
    #
    def readline (self, timeout=0) :
        """
        read a line from the child.  This method will read data into the cache,
        and return whatever it finds up to (but not including) the first newline
        (\\\\n).  When timeout is met, the method will return None, and leave 
        all data in the cache::

          timeout <  0: reads are blocking until data arrive, and call will
                        only return when any complete line has been found (which
                        may be never)

          timeout == 0: reads do not block, and the call will only be successful
                        if a complete line is already in the cache, or is found
                        on the first read attempt.

          timeout >  0: read calls block up to timeout, and reading is attempted
                        until timeout is reached, or a complete line is found,
                        whatever comes first.

        Note: the returned lines get '\\\\r' stripped.
        """

        # start the timeout timer right now.  Note that even if timeout is
        # short, and child.poll is slow, we will nevertheless attempt at least
        # one read...
        start = time.time ()

        if not self.alive () :
            raise se.NoSuccess ("Could not read line - pty process died")


        # check if we still have a full line in cache
        # FIXME: what happens if cache == '\n' ?
        if '\n' in self.cache :

            idx = self.cache.index ('\n')
            ret = self.cache[:idx-1]
            rem = self.cache[idx+1:]
            self.cache = rem  # store the remainder back into the cache
            return ret.replace('\r', '')


        try :
            # the cache is depleted, we need to read new data until we find
            # a newline, or until timeout
            while True :
            
                # do an idle wait 'til the next data chunk arrives
                rlist = []
                if timeout < 0 :
                    rlist, _, _ = select.select ([self.parent_out], [], [])
                else :
                    rlist, _, _ = select.select ([self.parent_out], [], [], timeout)

                # got some data - read them into the cache
                for f in rlist:
                    buf         = os.read (f, _CHUNKSIZE)
                    self.clog  += buf
                    self.cache += buf

                    buf = buf.replace ('\n', '\\n')
                    buf = buf.replace ('\r', '')
                    if  len(buf) > 40 :
                        self.logger.debug ("read : [%5d] (%s ... %s)" \
                                        % (len(buf), buf[:20], buf[-20:]))
                    else :
                        self.logger.debug ("read : [%5d] (%s)" \
                                        % (len(buf), buf))

                # check if we *now* have a full line in cache
                if '\n' in self.cache :

                    idx = self.cache.index ('\n')
                    ret = self.cache[:idx-1]
                    rem = self.cache[idx+1:]
                    self.cache = rem  # store the remainder back into the cache
                    return ret.replace('\r', '')

                # if not, check if we hit timeout
                now = time.time ()
                if (now-start) > timeout :
                    # timeout, but nothing found -- leave cache alone and return
                    return None


        except Exception as e :
            raise se.NoSuccess ("read from pty process failed (%s)" % e)



    # ----------------------------------------------------------------
    #
    def findline (self, patterns, timeout=0) :
        """
        This methods reads lines from the child process until a line matching
        any of the given patterns is found.  If that is found, all read lines
        (minus the matching one) are returned as a list of lines, the matching
        line itself is guaranteed to be the last line of the list.  This call
        never returns an empty list (the matching line is at least a linebreak).

        Note: the returned lines get '\\\\r' stripped.
        """


        if not self.alive () :
            raise se.NoSuccess ("Could not find line - pty process died")


        try :
            start = time.time ()            # startup timestamp
            ret   = []                      # array of read lines
            patts = []                      # compiled patterns
            line  = self.readline (timeout) # first line to check

            # pre-compile the given pattern, to speed up matching
            for pattern in patterns :
                patts.append (re.compile (pattern))

            # we wait forever -- there are two ways out though: a line matches
            # a pattern, or timeout passes
            while True :

                if not self.alive () :
                    break

                # time.sleep (0.1)

                # skip non-lines
                if  None == line :
                    line = self.readline (timeout)
                    continue

                # check current line for any matching pattern
                for n in range (0, len(patts)) :
                    if patts[n].search (line) :
                        # a pattern matched the current line: return a tuple of
                        # pattern index, matching line, and previous lines.
                        return (n, line.replace('\r', ''), ret)

                # if a timeout is given, and actually passed, return a non-match
                # and the set of lines found so far
                if timeout > 0 :
                    now = time.time ()
                    if (now-start) > timeout :
                        return (None, None, ret)

                # append current (non-matching) line to ret, and get new line 
                ret.append (line.replace('\r', ''))
                line = self.readline (timeout)

        except Exception as e :
            raise se.NoSuccess ("readline from pty process failed (%s)" % e)



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

        if not self.alive () :
            raise se.NoSuccess ("Could not find data - pty process died")

        try :
            start = time.time ()                       # startup timestamp
            ret   = []                                 # array of read lines
            patts = []                                 # compiled patterns
            data  = self.cache                         # initial data to check

            if not data : # empty cache?
                data = self.read (_CHUNKSIZE, _POLLDELAY)

            # pre-compile the given pattern, to speed up matching
            for pattern in patterns :
                patts.append (re.compile (pattern, re.MULTILINE | re.DOTALL))

            # we wait forever -- there are two ways out though: data matches
            # a pattern, or timeout passes
            while True :

              # time.sleep (0.1)

                # skip non-lines
                if  None == data :
                    data += self.read (_CHUNKSIZE, _POLLDELAY)

                # check current data for any matching pattern
              # print ">>%s<<" % data
                for n in range (0, len(patts)) :
                    match = patts[n].search (data)
                  # print "==%s==" % patterns[n]
                    if match :
                      # print "~~match!~~ %s" % data[match.start():match.end()]
                      # print "~~match!~~ %s" % (len(data))
                      # print "~~match!~~ %s" % (str(match.span()))
                        # a pattern matched the current data: return a tuple of
                        # pattern index and matching data.  The remainder of the
                        # data is cached.
                        ret  = data[0:match.end()+1]
                        self.cache = data[match.end()+1:] 
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
                data += self.read (_CHUNKSIZE, _POLLDELAY)


        except Exception as e :
            raise se.NoSuccess ("find from pty process failed (%s)" % e)



    # ----------------------------------------------------------------
    #
    def write (self, data) :
        """
        This method will repeatedly attempt to push the given data into the
        child's stdin pipe, until it succeeds to write all data.
        """

        if not self.alive () :
            raise se.NoSuccess ("Could not write data - pty not alive")

        try :

            buf = data.replace ('\n', '\\n')
            buf =  buf.replace ('\r', '')
            if  len(buf) > 40 :
                self.logger.debug ("write: [%5d] (%s ... %s)" \
                                % (len(data), buf[:20], buf[-20:]))
            else :
                self.logger.debug ("write: [%5d] (%s)" \
                                % (len(data), buf))

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
            raise se.NoSuccess ("write to pty process failed (%s)" % e)


    # ----------------------------------------------------------------
    #
    def _get_cache (self) :
        """
        Return the currently cached output
        """

        return self.cache


    # ----------------------------------------------------------------
    #
    def _get_cache_log (self) :
        """
        Return the currently cached output
        """

        return self.clog



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

