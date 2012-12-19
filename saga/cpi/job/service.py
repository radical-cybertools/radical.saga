
from saga.cpi.base import Base

# from saga.Url           import Url
# from saga.Object        import Object 
# from saga.Task          import Async
# from saga.exceptions    import *
# from saga.engine.config import Configurable

import saga.exceptions

# class Service (Object, Async, Configurable) :
class Service (Base) :

    def __init__ (self, api) : 
        raise saga.exceptions.NotImplemented ("method not implemented")

    def init_instance (self, rm, session) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def init_instance_async (self, rm, session, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def create_job (self, jd, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def run_job (self, cmd, host, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def list (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_url (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_job (self, job_id, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_self (self, ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

