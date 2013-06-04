import sys
import saga

try :
  from pudb import set_interrupt_handler; set_interrupt_handler()
except :
  pass

def main () :

    try:
        i  = 0
        js = saga.job.Service ("fork://localhost/")

        while True :

            i = i+1
            j = js.run_job ("/bin/true")

            print "%5d : %-30s : %s" % (i, j.id, j.state)

            j.wait ()
    
        return 0

    except saga.SagaException, ex:
        print "An exception occured: (%s) %s " % (ex.type, (str(ex)))
        print " \n*** Backtrace:\n %s" % ex.traceback
        return -1


if __name__ == "__main__":
    sys.exit(main())

