

import inspect
import saga.exceptions

class Async (object) :
    
    def __init__ (self, api) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def task_run (self, task) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def task_wait (self, task, timeout) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))

    def task_cancel (self, task) :
        raise saga.exceptions.NotImplemented ("%s.%s is not implemented" % (__name__,inspect.stack()[0][3]))


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

