
# from saga.Url    import Url
# from saga.Object import Object 
# from saga.Task   import Async

from saga.engine.logger import getLogger
from saga.engine.engine import getEngine

# class Service (Object, Async) :
class Service (object) :

    def __init__ (self, rm=None, session=None) : 
        '''
        rm:        saga.Url
        session:   saga.Session
        ret:       obj
        '''

        self._logger = getLogger ('saga.job.Job')
        self._logger.debug ("saga.job.Job.__init__(%s)" % id)

        self._engine = getEngine ()

        self._adaptor = self._engine.get_adaptor ('saga.job.Service', 'fork', rm, session)


    def create     (self, rm=None, session=None, ttype=None) : pass 
    #   rm:        saga.Url
    #   session:   saga.Session
    #   ttype:     saga.task.type enum
    #   ret:       saga.Task

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

