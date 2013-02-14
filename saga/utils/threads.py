
import threading

import saga.utils.exception


NEW     = 'New'
RUNNING = 'Running'
FAILED  = 'Failed'
DONE    = 'Done'


class Thread (threading.Thread) :

    def __init__ (self, call, *args, **kwargs) :

        if not callable (call) :
            raise saga.exceptions.BadParameter ("Thread requires a callable to function, not %s" \
                                             % (str(call)))

        threading.Thread.__init__ (self)

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
        t.run ()
        return t


    def run (self) :

        try :
            self._state     = RUNNING
            self._result    = self._call (*self._args, **self._kwargs)
            self._state     = DONE

        except Exception as e :
            self._exception = e
            self._traceback = saga.utils.exception.get_traceback ()
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


    def get_traceback (self) :

        if not self._state == FAILED :
            return None

        return self._traceback 

    traceback = property (get_traceback)



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

