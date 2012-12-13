
from saga.Url    import Url
from saga.Object import Object 
from saga.Task   import Async

class Service (Object, Async) :

    def __init__   (self, rm=None, session=None)             : pass 
    #   rm:        saga.Url
    #   session:   saga.Session
    #   ret:       obj

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

    def get_job    (self, job_id,          ttype=None)       : pass 
    #   job_id:    string
    #   ttype:     saga.task.type enum
    #   ret:       saga.job.Job / saga.Task

    def get_self   (self,                  ttype=None)       : pass 
    #   ttype:     saga.task.type enum
    #   ret:       saga.job.Self / saga.Task


    jobs = property (list)      # list [saga.job.Job]    # FIXME: dict {string id : saga.job.Job} ?
    self = property (get_self)  # saga.job.Self

