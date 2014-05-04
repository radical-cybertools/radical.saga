
import os
import sys
import saga

def main():
    """This code fails consistently after 16 copy opertaions:

            Copied file 0
            Copied file 1
            Copied file 2
            Copied file 3
            Copied file 4
            Copied file 5
            Copied file 6
            Copied file 7
            Copied file 8
            Copied file 9
            Copied file 10
            Copied file 11
            Copied file 12
            Copied file 13
            Copied file 14
            Copied file 15
    """

    try:
        for t in range(0, 32):
            f = saga.filesystem.File("sftp://login1.stampede.tacc.utexas.edu//etc/passwd")
            f.copy("file://localhost//tmp/passwd-%s" % t)
            print "Copied file %s" % t

            f.close()

        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1


if __name__ == "__main__":
    sys.exit(main())

