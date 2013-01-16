
from saga.attributes import *
from saga.base       import Base
from saga.constants  import *


class Context (Base, Attributes) :

    def __init__ (self, type=None, _adaptor=None, _adaptor_state={}) : 
        '''
        type: string
        ret:  None
        '''

        Base.__init__ (self, type.lower(), _adaptor, _adaptor_state, type, ttype=None)

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        self._attributes_register  (TYPE,            None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (SERVER,          None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (CERT_REPOSITORY, None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (USER_PROXY,      None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (USER_CERT,       None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (USER_KEY,        None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (USER_ID,         None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (USER_PASS,       None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (USER_VO,         None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (LIFE_TIME,       -1,   INT,    SCALAR, WRITEABLE)
        self._attributes_register  (REMOTE_ID,       None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (REMOTE_HOST,     None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (REMOTE_PORT,     None, STRING, VECTOR, WRITEABLE)


    def _initialize (self, session) :
        '''
        ret:  None
        '''
        return self._adaptor._initialize (session)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

