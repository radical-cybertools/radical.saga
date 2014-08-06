
import os
import sys
import saga

def main():
    """This code fails with the following error:
 
       copy from /etc//passwd to file://localhost/tmp/passwd-from-stampede is not supported
    """

    try:
        src = saga.filesystem.Directory  ("sftp://localhost/")
        tgt = saga.filesystem.Directory  ("file://localhost/tmp/copy_test",    flags=saga.filesystem.CREATE_PARENTS)
        copy_task = src.copy ("/tmp/stage*", "file://localhost/tmp/copy_test", flags=saga.filesystem.RECURSIVE, 
                                                                               ttype=saga.task.SYNC)

        copy_task.wait ()
        print copy_task.state
        print copy_task.files_copied

        return 0

    except saga.SagaException, ex:
        # Catch all saga exceptions
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        # Trace back the exception. That can be helpful for debugging.
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1


if __name__ == "__main__":
    sys.exit(main())

