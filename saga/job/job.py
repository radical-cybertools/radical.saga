

# from saga.Object        import Object
# from saga.Task          import Async 
# from saga.Permissions   import Permissions
# from saga.Attributes    import Attributes

from saga.engine.logger import getLogger
from saga.engine.engine import getEngine

def create_job (id=None, session=None, ttype=None) :
    #   id:        string
    #   session:   saga.Session
    #   ttype:     saga.task.type enum
    #   ret:       saga.Task
    pass

# class Job (Object, Async, Attributes, Permissions) :
class Job (object) :
    
    def __init__(self, id=None):
    
        # # set attribute interface properties
        # self.attributes_extensible_  (False)
        # self.attributes_camelcasing_ (True)

        # # register properties with the attribute interface 
        # self.attributes_register_   ('State',      self.Unknown, self.Enum,   self.Scalar, self.ReadOnly)
        # self.attributes_register_   ('Exitcode',   None,         self.Int,    self.Scalar, self.ReadOnly)
        # self.attributes_register_   ('JobID',      None,         self.String, self.Scalar, self.ReadOnly)
        # self.attributes_register_   ('ServiceURL', None,         self.Url,    self.Scalar, self.ReadOnly)

        # self.attributes_set_enums_  ('State',   [UNKNOWN, NEW, RUNNING, DONE,
        #                                          FAILED,  CANCELED, SUSPENDED])

        # self.attributes_set_getter_ ('State',    self.get_state)
        # self.attributes_set_getter_ ('jobID',    self.get_job_id)
        # self.attributes_set_getter_ ('Exitcode', self.get_exitcode_)

        self._logger = getLogger ('saga.job.Job')
        self._logger.debug ("saga.job.Job.__init__(%s)" % id)

        self._engine = getEngine ()

        self._adaptor = self._engine.get_adaptor ('saga.job.Job', 'local', id)


    def create          (self, id=None,  ttype=None) : pass 
    #   id:        string
    #   ttype:     saga.task.type enum
    #   ret:       saga.Task  

    def get_description (self,           ttype=None) : pass 
    #   ttype:     saga.task.type enum
    #   ret:       saga.job.Description / saga.Task  

    def get_stdin  (self,                ttype=None) : pass 
    #   ttype:     saga.task.type enum
    #   ret:       os.File / saga.Task

    def get_stdout (self,                ttype=None) : pass 
    #   ttype:     saga.task.type enum
    #   ret:       os.File / saga.Task

    def get_stderr (self,                ttype=None) : pass 
    #   ttype:     saga.task.type enum
    #   ret:       os.File / saga.Task

    def suspend    (self,                ttype=None) : pass 
    #   ttype:     saga.task.type enum
    #   ret:       None / saga.Task

    def resume     (self,                ttype=None) : pass 
    #   ttype:     saga.task.type enum
    #   ret:       None / saga.Task

    def checkpoint (self,                ttype=None) : pass 
    #   ttype:     saga.task.type enum
    #   ret:       None / saga.Task

    def migrate    (self, jd,            ttype=None) : pass 
    #   jd:        saga.job.Description  
    #   ttype:     saga.task.type enum
    #   ret:       None / saga.Task

    def signal     (self, signum,        ttype=None) : pass 
    #   signum:    int
    #   ttype:     saga.task.type enum
    #   ret:       None / saga.Task


    description = property (get_description)  # Description
    stdin       = property (get_stdin)        # os.File
    stdout      = property (get_stdout)       # os.File
    stderr      = property (get_stderr)       # os.File


    #-----------------------------------------------------------------
    #
    # task methods flattened into job :-/
    #
    def run         (self,               ttype=None) : pass 
    #   ret:        None

    def cancel      (self, timeout=None, ttype=None) : pass 
    #   timeout:    float
    #   ret:        None

    def wait        (self, timeout=-1,   ttype=None) : pass 
    #   timeout:    float 
    #   ret:        None

    def get_state   (self,               ttype=None) : pass 
    #   ret:        Task/Job state enum

    def get_result  (self,               ttype=None) : pass 
    #   ret:        <result type>
    #   note:       this will always return None for a job.

    def get_object  (self,               ttype=None) : pass 
    #   ret:        <object type>
    #   note:       this will return the job_service which created the job.

    def re_raise    (self,               ttype=None) : pass 
    #   ret:        <exception type>
    #   note:       if job failed, that will re-raise an exception describing 
    #               why, if that exists.  Otherwise, the call does nothing.


    state     = property (get_state)       # state enum
    result    = property (get_result)      # result type    (None)
    object    = property (get_object)      # object type    (job_service)
    exception = property (re_raise)        # exception type



def create_self (session=None, ttype=None) :
    #   session:   saga.Session
    #   ttype:     saga.task.type enum
    #   ret:       saga.Task
    pass


# class Self (Job, monitoring.Steerable) :
class Self (Job) :

    def __init__(self):
    
        # set attribute interface properties
        self.attributes_extensible_  (False)
        self.attributes_camelcasing_ (True)

    pass

