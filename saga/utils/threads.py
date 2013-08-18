
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import sys
import threading
import saga.exceptions  as se
import saga.utils.misc  as sumisc

_out_lock = threading.RLock ()

# ------------------------------------------------------------------------------
#
NEW     = 'New'
RUNNING = 'Running'
FAILED  = 'Failed'
DONE    = 'Done'


# ------------------------------------------------------------------------------
#
def lout (txt, stream=sys.stdout) :

    with _out_lock :
        stream.write (txt)
        stream.flush ()



# ------------------------------------------------------------------------------
#
class Thread (threading.Thread) : pass

def Event (*args, **kwargs) :
    return threading.Event (*args, **kwargs)

# ------------------------------------------------------------------------------
#
class RLock (object) :
    # see http://stackoverflow.com/questions/6780613/
    #     is-it-possible-to-subclass-lock-objects-in-python-if-not-other-ways-to-debug

# ------------------------------------------------------------------------------
#
    def __init__ (self, obj=None) :

        self._lock = threading.RLock ()

      # with self._lock :
      #     self._obj = obj
      #     self._cnt = 0


# ------------------------------------------------------------------------------
#
    def acquire (self) :

      # ind = (self._cnt)*' '+'>'+(30-self._cnt)*' '
      # lout ("%s -- %-10s %50s acquire  - %s\n" % (ind, threading.current_thread().name, self, self._lock))

        self._lock.acquire ()

      # self._cnt += 1
      # ind = (self._cnt)*' '+'|'+(30-self._cnt)*' '
      # lout ("%s    %-10s %50s acquired - %s\n" % (ind, threading.current_thread().name, self, self._lock))


# ------------------------------------------------------------------------------
#
    def release (self) :

      # ind = (self._cnt)*' '+'-'+(30-self._cnt)*' '
      # lout ("%s    %-10s %50s release  - %s\n" % (ind, threading.current_thread().name, self, self._lock))

        self._lock.release ()

      # self._cnt -= 1
      # ind = (self._cnt)*' '+'<'+(30-self._cnt)*' '
      # lout ("%s -- %-10s %50s released - %s\n" % (ind, threading.current_thread().name, self, self._lock))


# ------------------------------------------------------------------------------
#
    def __enter__ (self)                         : self.acquire () 
    def __exit__  (self, type, value, traceback) : self.release ()



# ------------------------------------------------------------------------------
#
class SagaThread (Thread) :

    def __init__ (self, call, *args, **kwargs) :

        if not callable (call) :
            raise se.BadParameter ("Thread requires a callable to function, not %s" \
                                % (str(call)))

        Thread.__init__ (self)

        self._call      = call
        self._args      = args
        self._kwargs    = kwargs
        self._state     = NEW
        self._result    = None
        self._exception = None
        self.daemon     = True


    @classmethod
    def Run (self, call, *args, **kwargs) :

        t = self (call, *args, **kwargs)
        t.start ()
        return t


    @property 
    def tid (self) :
        return self.tid


    def run (self) :

        try :
            self._state     = RUNNING
            self._result    = self._call (*self._args, **self._kwargs)
            self._state     = DONE

        except Exception as e :
            print ' ========================================== '
            print repr(e)
            print ' ========================================== '
            print str(e)
            print ' ========================================== '
            print sumisc.get_trace ()
            print ' ========================================== '

            self._exception = e
            self._state     = FAILED


    def wait (self) :

        if self.isAlive () :
            self.join ()


    def cancel (self) :
        # FIXME: this is not really implementable generically, so we ignore 
        # cancel requests for now.
        pass


    def get_state (self) :
        return self._state 

    state = property (get_state)


    def get_result (self) :

        if not self._state == DONE :
            return None

        return self._result 

    result = property (get_result)


    def get_exception (self) :

        if not self._state == FAILED :
            return None

        return self._exception 

    exception = property (get_exception)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

