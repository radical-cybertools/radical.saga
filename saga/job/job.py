

# from saga.Object        import Object
# from saga.Task          import Async 
# from saga.Permissions   import Permissions
# from saga.Attributes    import Attributes

from saga.engine.logger import getLogger
from saga.engine.engine import getEngine, ANY_ADAPTOR

def create_job (id=None, session=None, ttype=None) :
    '''
    id:        string
    session:   saga.Session
    ttype:     saga.task.type enum
    ret:       saga.Task
    '''

    logger = getLogger ('saga.job.create_job')
    logger.debug ("saga.job.create_job (%s, %s, %s)"  \
               % (str(id), str(session), str(ttype)))

    engine = getEngine ()

    # attempt to find a suitable adaptor, which will call 
    # init_instance_async(), which returns a task as expected.
    return engine.get_adaptor ('saga.job.Job', 'fork', \
                               ttype, ANY_ADAPTOR, id, session)

def _create_job_from_adaptor (id, session, schema, adaptor_name) :
    '''
    id:           String
    session:      saga.Session
    schema:       String
    adaptor_name: String
    ret:          saga.job.Job (bound to a specific adaptor)
    '''

    logger = getLogger ('saga.job._create_job_from_adaptor')
    logger.debug ("saga.job._create_job_from_adaptor (%s, %s, %s,  %s)"  \
               % (id, str(session), schema, adaptor_name))

    engine = getEngine ()

    # attempt to find a suitable adaptor, which will call 
    # init_instance_sync(), resulting in 
    adaptor = engine.get_adaptor ('saga.job.Job', 'fork', None, 
                                  adaptor_name, id, session)

    return Job (id, _adaptor=adaptor)



# class Job (Object, Async, Attributes, Permissions) :
class Job (object) :
    
    def __init__(self, id=None, _adaptor=None):
    
        # # set attribute interface properties
        # self._attributes_extensible  (False)
        # self._attributes_camelcasing (True)

        # # register properties with the attribute interface 
        # self._attributes_register   ('State',      self.Unknown, self.Enum,   self.Scalar, self.ReadOnly)
        # self._attributes_register   ('Exitcode',   None,         self.Int,    self.Scalar, self.ReadOnly)
        # self._attributes_register   ('JobID',      None,         self.String, self.Scalar, self.ReadOnly)
        # self._attributes_register   ('ServiceURL', None,         self.Url,    self.Scalar, self.ReadOnly)

        # self._attributes_set_enums  ('State',   [UNKNOWN, NEW, RUNNING, DONE,
        #                                          FAILED,  CANCELED, SUSPENDED])

        # self._attributes_set_getter ('State',    self.get_state)
        # self._attributes_set_getter ('jobID',    self.get_job_id)
        # self._attributes_set_getter ('Exitcode', self._get_exitcode)

        self._logger = getLogger ('saga.job.Job')
        self._logger.debug ("saga.job.Job.__init__(%s)" % id)

        self._engine = getEngine ()

        if _adaptor :
            # created from adaptor
            self._adaptor = _adaptor
        else :
            # create from API -- create and bind adaptor
            self._adaptor = self._engine.get_adaptor ('saga.job.Job', 'local',
                                                      None, ANY_ADAPTOR, id)


    def get_id (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       String / saga.Task  
        '''
        return self._adaptor.get_id (ttype=ttype)


    def get_description (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       saga.job.Description / saga.Task  
        '''
        return self._adaptor.get_description (ttype=ttype)


    def get_stdin (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       os.File / saga.Task
        '''
        return self._adaptor.get_stdin (ttype=ttype)


    def get_stdout (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       os.File / saga.Task
        '''
        return self._adaptor.get_stdout (ttype=ttype)


    def get_stderr (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       os.File / saga.Task
        '''
        return self._adaptor.get_stderr (ttype=ttype)


    def suspend (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       None / saga.Task
        '''
        return self._adaptor.suspend (ttype=ttype)


    def resume (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       None / saga.Task
        '''
        return self._adaptor.resume (ttype=ttype)


    def checkpoint (self, ttype=None) :
        '''
        ttype:     saga.task.type enum
        ret:       None / saga.Task
        '''
        return self._adaptor.checkpoint (ttype=ttype)


    def migrate (self, jd, ttype=None) :
        '''
        jd:        saga.job.Description  
        ttype:     saga.task.type enum
        ret:       None / saga.Task
        '''
        return self._adaptor.migrate (jd, ttype=ttype)


    def signal (self, signum, ttype=None) :
        '''
        signum:    int
        ttype:     saga.task.type enum
        ret:       None / saga.Task
        '''
        return self._adaptor.signal (signum, ttype=ttype)


    description = property (get_description)  # Description
    stdin       = property (get_stdin)        # os.File
    stdout      = property (get_stdout)       # os.File
    stderr      = property (get_stderr)       # os.File


    #-----------------------------------------------------------------
    #
    # task methods flattened into job :-/
    #
    def run (self, ttype=None) :
        '''
        ret:        None
        '''
        return self._adaptor.run (ttype=ttype)


    def cancel (self, timeout=None, ttype=None) :
        '''
        timeout:    float
        ret:        None
        '''
        return self._adaptor.cancel (timeout, ttype=ttype)


    def wait (self, timeout=-1, ttype=None) :
        '''
        timeout:    float 
        ret:        None
        '''
        return self._adaptor.wait (timeout, ttype=ttype)


    def get_state (self, ttype=None) :
        '''
        ret:        Task/Job state enum
        '''
        return self._adaptor.get_state (ttype=ttype)


    def get_result (self, ttype=None) :
        '''
        ret:        <result type>
        note:       this will always return None for a job.
        '''
        return self._adaptor.get_result (ttype=ttype)


    def get_object (self, ttype=None) :
        '''
        ret:        <object type>
        note:       this will return the job_service which created the job.
        '''
        return self._adaptor.get_object (ttype=ttype)


    def re_raise (self, ttype=None) :
        '''
        ret:        <exception type>
        note:       if job failed, that will re-raise an exception describing 
                    why, if that exists.  Otherwise, the call does nothing.
        '''
        return self._adaptor.re_raise (ttype=ttype)


    state     = property (get_state)       # state enum
    result    = property (get_result)      # result type    (None)
    object    = property (get_object)      # object type    (job_service)
    exception = property (re_raise)        # exception type



def create_self (session=None, ttype=None) :
    '''
    session:   saga.Session
    ttype:     saga.task.type enum
    ret:       saga.Task
    '''

    logger = getLogger ('saga.job.create_self')
    logger.debug ("saga.job.create_self (%s, %s)"  \
               % (str(session), str(ttype)))

    engine = getEngine ()

    # attempt to find a suitable adaptor, which will call 
    # init_instance_async(), which returns a task as expected.
    return engine.get_adaptor ('saga.job.Self', 'fork', ttype, ANY_ADAPTOR, session)


# class Self (Job, monitoring.Steerable) :
class Self (Job) :

    def __init__(self, session=None):
    
        # # set attribute interface properties
        # self._attributes_extensible  (False)
        # self._attributes_camelcasing (True)

        self._logger = getLogger ('saga.job.Self')
        self._logger.debug ("saga.job.Self.__init__ (%s, %s)"  \
                         % (str(session)))

        self._engine = getEngine ()

        self._adaptor = self._engine.get_adaptor ('saga.job.Self', 'fork', \
                                                  SYNC, ANY_ADAPTOR, session)

