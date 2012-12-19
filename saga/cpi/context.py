
from saga.cpi.base import Base

import saga.exceptions

class Context (Base) :
    
    def __init__  (self, api) :
        raise saga.exceptions.NotImplemented


    def init_instance (self, type) :
        raise saga.exceptions.NotImplemented ("method not implemented")


    def _initialize (self, session) :
        raise saga.exceptions.NotImplemented ("method not implemented")

