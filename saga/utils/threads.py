
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import Queue
import threading

import saga.utils.exception

NEW     = 'New'
RUNNING = 'Running'
FAILED  = 'Failed'
DONE    = 'Done'

class Thread (threading.Thread) :

    @classmethod
    def _call_wrapper (self, thread) :

        try :
            thread._state     = RUNNING
            thread._result    = thread._call (*thread._args, **thread._kwargs)
            thread._state     = DONE

        except Exception as e :
            thread._exception = e
            thread._traceback = saga.utils.exception.get_traceback ()
            thread._state     = FAILED



    def __init__ (self, call, *args, **kwargs) :

        threading.Thread.__init__ (self)
        self._call      = call
        self._args      = args
        self._kwargs    = kwargs
        self._state     = NEW
        self._queue     = Queue.Queue ()
        self._result    = None
        self._exception = None

        self.daemon     = True
        self.start ()


    def run (self) :

        Thread._call_wrapper (self)


    def get_state (self) :

        return self._state 


    def get_result (self) :

        if not self._state == DONE :
            return None

        return self._result 


    def get_exception (self) :

        if not self._state == FAILED :
            return None

        return self._exception 


    def get_traceback (self) :

        if not self._state == FAILED :
            return None

        return self._traceback 


