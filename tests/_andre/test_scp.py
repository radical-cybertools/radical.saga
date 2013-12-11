
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import re
import time
import saga
import saga.utils.pty_shell as sups

try :
    shell = sups.PTYShell (saga.Url ("fork://localhost"), [])
    shell.run_async ("scp ~/downloads/totalview*.sh @localhost:/tmp/t")

  # pat_bof = re.compile ("(?P<perc>\d+\%).*(?P<time>--:--)\s*ETA")
    pat_bof = re.compile ("(?P<perc>\d+)\%\s+(?P<size>.+?)\s+(?P<perf>.+?)\s+(?P<time>--:--)\s*ETA")
    pat_eta = re.compile ("(?P<perc>\d+)\%\s+(?P<size>.+?)\s+(?P<perf>.+?)\s+(?P<time>\d\d:\d\d)\s*ETA")
    pat_eof = re.compile ("(?P<perc>\d+)\%\s+(?P<size>.+?)\s+(?P<perf>.+?)\s+(?P<time>\d\d:\d\d)\s*\n")

    begin = True
    error = ""

    while True :
        ret, out = shell.find (['ETA', '\n'])

        match = None

        if  ret == 0 :
            if  begin :
                match = pat_bof.search (out)
                begin = False
            else :
                match = pat_eta.search (out)

        if  ret == 1 :
            match = pat_eof.search (out)

        if not match :
            # parsing error
            error += out
            break

        print "%6s%%  %6s  %10s  %6s" % (match.group ('perc'), 
                                         match.group ('size'),
                                         match.group ('perf'),
                                         match.group ('time'))
        if  ret == 1 :
            break

    ret, out = shell.find_prompt ()
    if ret != 0 :
        print "file copy failed:\n'%s'" % error
    else :
        print "file copy done"


except saga.SagaException as e :
    print "exception: %s" % e




