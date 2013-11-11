
import os
import sys
import saga

def main():
    """This code fails with the following error:
 
       copy from /etc//passwd to file://localhost/tmp/passwd-from-stampede is not supported
    """

    try:
        d = saga.filesystem.Directory("sftp://login1.stampede.tacc.utexas.edu/")
        d.copy("/etc//passwd", "file://localhost/tmp/passwd-from-stampede")

        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1


if __name__ == "__main__":
    sys.exit(main())

