
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

# from   saga.engine.logger import Logger, getLogger

import Queue
import threading



class Thread (threading.Thread) :


    def __init__ (self, call, *args, **kwargs) :

        threading.Thread.__init__ (self)

 #      Logger()
 #      self._logger    = getLogger('saga.utils.thread')

        self._call      = call
        self._args      = args
        self._kwargs    = kwargs
        self._queue     = Queue.Queue ()
        self._result    = None
        self._exception = None

        self.start ()


    @classmethod
    def _call_wrapper (self, thread) :

        try :
            thread._result = thread._call (thread._args, thread._kwargs)

        except Exception as e :
            thread._exception = e


    def run (self) :

        Thread._call_wrapper (self)

    def get_result (self) :
        return self._result 

