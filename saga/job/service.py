
# from saga.Url    import Url
# from saga.Object import Object 
# from saga.Task   import Async

from saga.engine.logger import getLogger
from saga.engine.engine import getEngine
from saga.task          import SYNC, ASYNC, TASK

def create_service (rm=None, session=None, ttype=None) :
    #   rm:        saga.Url
    #   session:   saga.Session
    #   ttype:     saga.task.type enum
    #   ret:       saga.Task

    logger = getLogger ('saga.job.create_service')
    logger.debug ("saga.job.create_service (%s, %s, %s)"  \
               % (str(rm), str(session), str(ttype)))

    engine = getEngine ()

    # attempt to find a suitable adaptor, and call 
    # init_instance_async(), which returns a task as expected.
    # The task is responsible for binding the adaptor to the returned API
    # instance.
    return engine.get_adaptor ('saga.job.Service', 'fork', \
                               ttype, rm, session)


# class Service (Object, Async) :
class Service (object) :

    def __init__ (self, rm=None, session=None) : 
        '''
        rm:        saga.Url
        session:   saga.Session
        ret:       obj
        '''

        self._logger = getLogger ('saga.job.Service')
        self._logger.debug ("saga.job.Service.__init__ (%s, %s)"  \
                         % (str(rm), str(session)))

        self._engine = getEngine ()

        self._adaptor = self._engine.get_adaptor ('saga.job.Service', 'fork', \
                                                  SYNC, rm, session)



    def create_job (self, jd,              ttype=None)       : pass 
    #   jd:        saga.job.Description
    #   ttype:     saga.task.type enum
    #   ret:       saga.job.Job / saga.Task

    def run_job    (self, cmd, host="",    ttype=None)       : pass 
    #   cmd:       string
    #   host:      string
    #   ttype:     saga.task.type enum
    #   ret:       saga.job.Job / saga.Task

    def list       (self,                  ttype=None)       : pass 
    #   ttype:     saga.task.type enum
    #   ret:       list [string] / saga.Task

    def get_url    (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       saga.job.Job / saga.Task
        '''
        return self._adaptor.get_url (ttype)


    def get_job    (self, job_id,          ttype=None)       : pass 
    #   job_id:    string
    #   ttype:     saga.task.type enum
    #   ret:       saga.job.Job / saga.Task

    def get_self   (self,                  ttype=None)       : pass 
    #   ttype:     saga.task.type enum
    #   ret:       saga.job.Self / saga.Task


    jobs = property (list)      # list [saga.job.Job]    # FIXME: dict {string id : saga.job.Job} ?
    self = property (get_self)  # saga.job.Self

