
import re
import os
import pty
import time
import select
import subprocess


# --------------------------------------------------------------------
#
#
class pty_process (object) :
    """
    This method spawns a process while providing that child with a pty
    """

    # ----------------------------------------------------------------
    #
    def __init__ (self, command) :

        if isinstance (command, basestring) :
            command = command.split (' ')

        self.command = command
        self.cache   = ""      # data cache
        self.chunk   = 1024    # size of each read
        self.child   = None


    # ----------------------------------------------------------------
    #
    def run (self) :

        self.master_in,  self.slave_in  = pty.openpty ()
        self.master_out, self.slave_out = pty.openpty ()
        self.master_err, self.slave_err = pty.openpty ()

        self.child = subprocess.Popen (self.command, 
                                       stdin   = self.slave_in,
                                       stdout  = self.slave_out, 
                                       stderr  = self.slave_out, 
                                       shell   = False,  
                                       bufsize = 1)

    # --------------------------------------------------------------------
    #
    # read a line from a given file descriptor.  The intermittent read result is
    # cached, and lines are actually extracted from that read cache as needed.
    def readline (self) :

        if not self.child :
            return None

        if self.child.poll () is not None :
            return None

        # check if we still have a full line in cache
        while not '\n' in self.cache :
        
            # if not, do an idle wait 'til the next data chunk arrives
            rlist, wlist, xlist = select.select([self.master_out], [], [], 0.01)

            # got some data - read them into the cache
            for f in rlist:
                buf = os.read (f, self.chunk)
                self.cache += buf

        # at this point, we are sure to have a newline in the cache, and thus can
        # extract a line from the beginning of the cache
        idx = self.cache.index ('\n')
        ret = self.cache[:idx-1]
        rem = self.cache[idx+1:]
        self.cache = rem  # store the remainder back into the cache
        
        return ret


    # ----------------------------------------------------------------
    #
    def findline (self, pattern, timeout=0) :
        """
        This methods reads lines from the child process until a line matching
        the given pattern is found.  If that is found, all read lines (minus the
        matching one) are returned as a list of lines, the matching line itself
        is guaranteed to be the last line of the list.  This call never returns
        an empty list (the matching line is at least a linebreak).
        """
        start = time.time ()
        patt  = re.compile (pattern)
        line  = self.readline ()
        ret   = [line]

        while not patt.search (ret[-1]) :
            line = self.readline ()
            ret.append (line)

            if timeout > 0 :
                now = time.time ()
                if (now-start) > timeout :
                    return None

        return ret


    # ----------------------------------------------------------------
    #
    def writeline (self, line) :

        if self.child.poll () is not None :
            return

        done = False
          
        while not done :

            rlist, wlist, xlist = select.select([], [self.master_in], [], 1)

            # got some data - read them into the cache
            for f in wlist:
                buf = os.write (f, "%s\n" % line)
                done = True


    # ----------------------------------------------------------------
    #
    def poll (self) :

        return self.child.poll ()



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

