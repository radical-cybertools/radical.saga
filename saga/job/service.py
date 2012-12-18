
# from saga.Url    import Url
# from saga.Object import Object 
# from saga.Task   import Async

from saga.engine.logger import getLogger
from saga.engine.engine import getEngine
from saga.task          import SYNC, ASYNC, TASK

def create_service (rm=None, session=None, ttype=None) :
    '''
    rm:        saga.Url
    session:   saga.Session
    ttype:     saga.task.type enum
    ret:       saga.Task
    '''

    logger = getLogger ('saga.job.create_service')
    logger.debug ("saga.job.create_service (%s, %s, %s)"  \
               % (str(rm), str(session), str(ttype)))

    engine = getEngine ()

    # attempt to find a suitable adaptor, which will call 
    # init_instance_async(), which returns a task as expected.
    return engine.get_adaptor ('saga.job.Service', 'fork', \
                               ttype, None, rm, session)


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
                                                  SYNC, None, rm, session)


    def create_job (self, jd, ttype=None) :
        '''
        jd:        saga.job.Description
        ttype:     saga.task.type enum
        ret:       saga.job.Job / saga.Task
        '''
        return self._adaptor.create_job (jd, ttype)


    def run_job (self, cmd, host="", ttype=None) :
        '''
        cmd:       string
        host:      string
        ttype:     saga.task.type enum
        ret:       saga.job.Job / saga.Task
        '''
        return self._adaptor.run_job (cmd, host, ttype)


    def list (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       list [string] / saga.Task
        '''
        return self._adaptor.list (ttype)


    def get_url (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       saga.job.Job / saga.Task
        '''
        return self._adaptor.get_url (ttype)


    def get_job (self, job_id, ttype=None) :
        '''
        job_id:    string
        ttype:     saga.task.type enum
        ret:       saga.job.Job / saga.Task
        '''
        return self._adaptor.get_job (job_id, ttype)


    def get_self (self,ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       saga.job.Self / saga.Task
        '''
        return self._adaptor.get_self (ttype)


    jobs = property (list)      # list [saga.job.Job]    # FIXME: dict {string id : saga.job.Job} ?
    self = property (get_self)  # saga.job.Self

