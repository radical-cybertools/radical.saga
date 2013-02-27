
import re
import time
import saga
import saga.utils.pty_shell as sups

try :
    shell = sups.PTYShell (saga.Url ("fork://localhost"), [])
    shell.run_async ("sftp -b - localhost <<EOT")
    shell.send ("progress\nput /home/merzky/downloads/totalview*.sh /tmp/t\nEOT\n")

  # pat_bof = re.compile ("(?P<perc>\d+\%).*(?P<time>--:--)\s*ETA")
    pat_bof = re.compile ("(?P<perc>\d+)\%\s+(?P<size>.+?)\s+(?P<perf>.+?)\s+(?P<time>--:--)\s*ETA")
    pat_eta = re.compile ("(?P<perc>\d+)\%\s+(?P<size>.+?)\s+(?P<perf>.+?)\s+(?P<time>\d\d:\d\d)\s*ETA")
    pat_eof = re.compile ("(?P<perc>\d+)\%\s+(?P<size>.+?)\s+(?P<perf>.+?)\s+(?P<time>\d\d:\d\d)\s*\n")
    pat_def = re.compile ("^sftp>.*\n")

    begin = True

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
                # ignore line echo
                continue

        if not match :
            print "parsing error"
            break

        print "%6s%%  %6s  %10s  %6s" % (match.group ('perc'), 
                                         match.group ('size'),
                                         match.group ('perf'),
                                         match.group ('time'))
        if  ret == 1 :
            break

    ret, txt = shell.find_prompt ()
    print "--%s-- : --%s--" % (ret, txt)


except saga.SagaException as e :
    print "exception: %s" % e


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

