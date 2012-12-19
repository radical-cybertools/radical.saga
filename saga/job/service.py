
# from saga.Url    import Url
# from saga.Object import Object 
# from saga.Task   import Async

from saga.engine.logger import getLogger
from saga.engine.engine import getEngine, ANY_ADAPTOR
from saga.task          import SYNC, ASYNC, TASK
from saga.url           import Url




# class Service (Object, Async) :
class Service (object) :

    def __init__ (self, rm=None, session=None) : 
        '''
        rm:        saga.Url
        session:   saga.Session
        ret:       obj
        '''

        rm_url = Url (rm)

        self._engine  = getEngine ()
        self._logger  = getLogger ('saga.job.Service')
        self._logger.debug ("saga.job.Service.__init__ (%s, %s)"  \
                         % (str(rm_url), str(session)))

        self._adaptor = self._engine.get_adaptor (self, 'saga.job.Service', rm_url.scheme, \
                                                  SYNC, ANY_ADAPTOR, rm_url, session)


    @classmethod
    def create (self, rm=None, session=None, ttype=None) :
        '''
        rm:        saga.Url
        session:   saga.Session
        ttype:     saga.task.type enum
        ret:       saga.Task
        '''
    
        rm_url = Url (rm)

        engine = getEngine ()
        logger = getLogger ('saga.job.Service')
        logger.debug ("saga.job.Service.create(%s, %s, %s)"  \
                   % (str(rm_url), str(session), str(ttype)))
    
        # attempt to find a suitable adaptor, which will call 
        # init_instance_async(), which returns a task as expected.
        return engine.get_adaptor (self, 'saga.job.Service', rm_url.scheme, \
                                   ttype, ANY_ADAPTOR, rm_url, session)


    def create_job (self, jd, ttype=None) :
        '''
        jd:        saga.job.Description
        ttype:     saga.task.type enum
        ret:       saga.job.Job / saga.Task
        '''
        return self._adaptor.create_job (jd, ttype=ttype)


    def run_job (self, cmd, host="", ttype=None) :
        '''
        cmd:       string
        host:      string
        ttype:     saga.task.type enum
        ret:       saga.job.Job / saga.Task
        '''
        return self._adaptor.run_job (cmd, host, ttype=ttype)


    def list (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       list [string] / saga.Task
        '''
        return self._adaptor.list (ttype=ttype)


    def get_url (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       saga.job.Job / saga.Task
        '''
        return self._adaptor.get_url (ttype=ttype)


    def get_job (self, job_id, ttype=None) :
        '''
        job_id:    string
        ttype:     saga.task.type enum
        ret:       saga.job.Job / saga.Task
        '''
        return self._adaptor.get_job (job_id, ttype=ttype)


    def get_self (self,ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       saga.job.Self / saga.Task
        '''
        return self._adaptor.get_self (ttype=ttype)


    jobs = property (list)      # list [saga.job.Job]    # FIXME: dict {string id : saga.job.Job} ?
    self = property (get_self)  # saga.job.Self

