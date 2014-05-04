
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import re
import time
import saga
import saga.utils.pty_shell as sups

try :
    shell = sups.PTYShell (saga.Url ("fork://localhost"), [])
    shell.run_async ("(sftp -b - localhost || (printf \"SFTP_ABORT\n\"; false)) <<EOT")
    shell.send ("progress\nput /home/merzky/downloads/totalview*.sh /tmp/t\nEOT\n")

  # pat_bof = re.compile ("(?P<perc>\d+\%).*(?P<time>--:--)\s*ETA")
    pat_bof = re.compile ("(?P<perc>\d+)\%\s+(?P<size>.+?)\s+(?P<perf>.+?)\s+(?P<time>--:--)\s*ETA")
    pat_eta = re.compile ("(?P<perc>\d+)\%\s+(?P<size>.+?)\s+(?P<perf>.+?)\s+(?P<time>\d\d:\d\d)\s*ETA")
    pat_eof = re.compile ("(?P<perc>\d+)\%\s+(?P<size>.+?)\s+(?P<perf>.+?)\s+(?P<time>\d\d:\d\d)\s*\n")
    pat_def = re.compile ("^sftp>.*\n")

    begin = True
    error   = ""

    while True :
        ret, out = shell.find (['ETA$', 'SFTP_ABORT\n', '\n'])
        progress    = None

        # ----------------------------------------------------------------------
        # found ETA - transfer is in progress
        if  ret == 0 :

            if  begin :
                # first ETA will look different than the others
                progress = pat_bof.search (out)
                begin = False
            else :
                progress = pat_eta.search (out)


        # ----------------------------------------------------------------------
        # found an ABORT?  well, then we abort and fetch prompt
        if  ret == 1 :
            break


        # ----------------------------------------------------------------------
        if  ret == 2 :
            progress = pat_eof.search (out)

            if not progress :
                # ignore line echo
                error += out
                continue

        # ----------------------------------------------------------------------
        # we should have found progress info on both ETA and '\n' matches...
        # oh well...
        if not progress :
            print "parse error -- ignore"
            error += out

        # ----------------------------------------------------------------------
        # had a match, either on 'ETA' or on '\n' -- both give progress info
        print "%6s%%  %6s  %10s  %6s" % (progress.group ('perc'), 
                                         progress.group ('size'),
                                         progress.group ('perf'),
                                         progress.group ('time'))
        # ----------------------------------------------------------------------
        # we had a match on '\n' -- this is the end of transfer
        if  ret == 2 :
            break

    ret, out = shell.find_prompt ()
    if ret != 0 :
        print "file copy failed:\n'%s'" % error
    else :
        print "file copy done"


except saga.SagaException as e :
    print "exception: %s" % e




