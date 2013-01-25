
import re
import os
import pty
import time
import select
import subprocess


# --------------------------------------------------------------------
#
_CHUNKSIZE = 1024   # default size of each read
_POLLDELAY = 0.0001 # seconds in between read attempts


# --------------------------------------------------------------------
#
class pty_process (object) :
    """
    This method spawns a process, providing that child with a pty.  It will then
    maintain stdin, stdout and stderr channels to the child.  All write*
    operations operate on the stdin, all read* operations operate on the stdout
    stream.  Data from the stderr stream are at this point not exposed.

    FIXME: never reading from stderr might fill the stderr buffer, effectively
           blocking the child process.  Reads should attempt to clean out the
           stderr stream (into an err cache?), or the class needs to make sure
           that stderr is discarded.

    Example::

        pty = pty_process ("/usr/bin/ssh -ttt localhost")
        pty.run ()

        (n, match) = pty.findstring (['password\s*:\s*$', 
                                           'want to continue connecting.*\(yes/no\)\s*$', 
                                           '[\$#>]\s*$'])

        while True :

            if n == 0 :
                # found password prompt
                pty.write ("secret\\n")
                (n, match) = pty.findstring (['password\s*:\s*$', 
                                             'want to continue connecting.*\(yes/no\)\s*$', 
                                             '[\$#>]\s*$'])
            elif n == 1 :
                # found request to accept host key
                pty.write ("yes\\n")
                (n, match) = pty.findstring (['password\s*:\s*$', 
                                             'want to continue connecting.*\(yes/no\)\s*$', 
                                             '[\$#>]\s*$'])
            elif n == 2 :
                # found some prompt
                break
        
        i = 0
        while pty.alive () is None:
            i += 1
            # send sleeps as quickly as possible, forever...
            (n, match) = pty.findstring (['[\$#>]\s*$'])
            pty.write ("/bin/sleep %d\\n" % i)
    """

    # ----------------------------------------------------------------
    #
    def __init__ (self, command) :
        """
        The pty class constructor.

        The given command is what is run as a child, and fed/drained via pty
        pipes.  If given as string, command is split into an array of strings
        (simple splis on white space), as that is what :func:`subprocess.Popen`
        wants.
        """

        if isinstance (command, basestring) :
            command = command.split (' ')

        self.command = command # list of strings too run()
        self.cache   = ""      # data cache
        self.child   = None    # the process as created by subprocess.Popen


        # create the pty pipes (two ends, one for this process, one for the
        # child process; tree pairs, for each of the in, out and err channels)
        self.master_in,  self.slave_in  = pty.openpty ()
        self.master_out, self.slave_out = pty.openpty ()
        self.master_err, self.slave_err = pty.openpty ()

        # run the child, feeding it the slave ends of the pty pipes...
        self.child = subprocess.Popen (self.command, 
                                       stdin   = self.slave_in,
                                       stdout  = self.slave_out, 
                                       stderr  = self.slave_out, 
                                       shell   = False,           # we don't run shell commands
                                       bufsize = 0)               # unbuffered I/O

    # --------------------------------------------------------------------
    #
    def alive (self) :
        """
        alive() checks if the child gave an exit value -- if none, it is assumed
        to be still alive.
        """

        if self.child.poll () is None :
            return True
        else :
            return False


    # --------------------------------------------------------------------
    #
    def read (self, size=_CHUNKSIZE, timeout=0) :
        """ 
        read some data from the child.  By default, the method reads a full
        chunk, but other read sizes can be specified.  
        
        The method will return whatever data is has at timeout::
        
          timeout == 0 : return the content of the first successful read, with
                         whatever data up to 'size' have been found.
          timeout <  0 : return after first read attempt, even if no data have been
                         available.

        If no data are found, the method returns an empty string (not None).

        This method will not fill the cache, but will just read whatever data it
        needs (FIXME).

        Note: the returned lines do *not* get '\\\\r' stripped.
        """

        # start the timeout timer right now.  Note that if timeout is short, and
        # child.poll is slow, we will nevertheless attempt at least one read...
        start = time.time ()

        if not self.child :
            return None

        if not self.alive () :
            return None

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


        # read until we have enough data, or hit timeout ceiling...
        while True :
        
            # do an idle wait 'til the next data chunk arrives, or 'til sel_to
            rlist, _, _ = select.select ([self.master_out], [], [], sel_to)

            # got some data? 
            for f in rlist:
                # read whatever we still need
                ret += os.read (f, size-len(ret))

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


        # we should never get here...


    # --------------------------------------------------------------------
    #
    def readline (self, timeout=0) :
        """
        read a line from the child.  This method will read data into the cache,
        and return whatever it finds up to (but not including) the first newline
        (\\\\n).  When timeout is met, the method will return None, and leave all
        data in the cache::

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

        # start the timeout timer right now.  Note that if timeout is short, and
        # child.poll is slow, we will nevertheless attempt at least one read...
        start = time.time ()

        if not self.child :
            return None

        if not self.alive () :
            return None


        # check if we still have a full line in cache
        if '\n' in self.cache :

            idx = self.cache.index ('\n')
            ret = self.cache[:idx-1]
            rem = self.cache[idx+1:]
            self.cache = rem  # store the remainder back into the cache
            return ret.replace('\r', '')


        # the cache is depleted, we need to read new data until we find
        # a newline, or until timeout
        while True :
        
            # do an idle wait 'til the next data chunk arrives
            rlist = []
            if timeout < 0 :
                rlist, _, _ = select.select ([self.master_out], [], [])
            else :
                rlist, _, _ = select.select ([self.master_out], [], [], timeout)


            # got some data - read them into the cache
            for f in rlist:
                self.cache += os.read (f, _CHUNKSIZE)

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
                # timeout hit, but nothing found -- leave cache alone and return
                return None

        # we should never get here



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

        start = time.time ()            # startup timestamp to compare timeout against
        ret   = []                      # array of read lines
        patts = []                      # compiled patterns
        line  = self.readline (timeout) # first line to check

        # pre-compile the given pattern, to speed up matching
        for pattern in patterns :
            patts.append (re.compile (pattern))

        # we wait forever -- there are two ways out though: a line matches
        # a pattern, or timeout passes
        while True :

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

            # if a timeout is given, and actually passed, return a non-match,
            # and the set of lines found so far
            if timeout > 0 :
                now = time.time ()
                if (now-start) > timeout :
                    return (None, None, ret)

            # append the current (non-matching) line to ret, and get a new line to test
            ret.append (line.replace('\r', ''))
            line = self.readline (timeout)



    # ----------------------------------------------------------------
    #
    def findstring (self, patterns, timeout=0) :
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

        Note that the pattern are interpreted with the re.M (multi-line) and
        re.S (dot matches all) regex flags.

        Performance: the call is doing repeated string regex searches over
        whatever data it finds.  On complex regexes, and large data, and small
        read buffers, this method can be expensive.  

        Note: the returned data get '\\\\r' stripped.
        """

        start = time.time ()                       # startup timestamp to compare timeout against
        ret   = []                                 # array of read lines
        patts = []                                 # compiled patterns
        data  = self.read (_CHUNKSIZE, _POLLDELAY) # initial data to check

        # pre-compile the given pattern, to speed up matching
        for pattern in patterns :
            patts.append (re.compile (pattern, re.MULTILINE | re.DOTALL))

        # we wait forever -- there are two ways out though: data matches
        # a pattern, or timeout passes
        while True :

            # skip non-lines
            if  None == data :
                data += self.read (_CHUNKSIZE, _POLLDELAY)
                continue

            # check current data for any matching pattern
            for n in range (0, len(patts)) :
                if patts[n].search (data) :
                    # a pattern matched the current data: return a tuple of
                    # pattern index and matching data.
                    return (n, data.replace('\r', ''))

            # if a timeout is given, and actually passed, return a non-match.
            if timeout > 0 :
                now = time.time ()
                if (now-start) > timeout :
                    return (None, None)

            # no match yet, still time -- read more data
            data += self.read (_CHUNKSIZE, _POLLDELAY)



    # ----------------------------------------------------------------
    #
    def write (self, data) :
        """
        This method will repeatedly attempt to push the given data into the
        child's stdin pipe, until it succeeds to write all data.
        """

        if not self.alive () :
            return

        # attempt to write forever -- until we succeeed
        while True :

            # check if the pty pipe is ready for data
            _, wlist, _ = select.select ([], [self.master_in], [], _POLLDELAY)

            for f in wlist :
                
                # write will report the number of written bytes
                ret = os.write (f, "%s" % data)

                # if all data are written, we are done
                if ret == len(data) :
                    return
                
                # otherwise, truncate by written data, and try again
                data = data[ret:]


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

