
from saga.cpi.base import Base

import saga.exceptions

class Context (Base) :
    
    def __init__  (self, api) :
        raise saga.exceptions.NotImplemented


    def init_instance (self, type) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def _initialize (self, session) :
        raise saga.exceptions.NotImplemented ("method not implemented")


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

