
from saga.cpi.base import Base

# from saga.cpi import Object      as CPIObject
# from saga.cpi import Async       as CPIAsync 
# from saga.cpi import Permissions as CPIPermissions
# from saga.cpi import Attributes  as CPIAttributes

import saga.exceptions

# class Job (CPIBase) : # CPIObject, CPIAsync, CPIAttributes, CPIPermissions) :
class Job (Base) :
    
    def __init__        (self, id                 ) :
        raise saga.exceptions.NotImplemented

    def create          (self, id,           ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_description (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_stdin       (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_stdout      (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_stderr      (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def suspend         (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def resume          (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def checkpoint      (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def migrate         (self, jd,           ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def signal          (self, signum,       ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    #-----------------------------------------------------------------
    # task methods flattened into job :-/
    def run             (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def cancel          (self, timeout,      ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def wait            (self, timeout,      ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_state       (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_result      (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def get_object      (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")

    def re_raise        (self,               ttype) :
        raise saga.exceptions.NotImplemented ("method not implemented")



# class Self (Job, monitoring.Steerable) :
class Self (Job) :

    def __init__(self):
        raise saga.exceptions.NotImplemented ("method not implemented")

