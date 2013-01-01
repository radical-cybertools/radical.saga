
import saga

from saga.engine.logger import getLogger
from saga.engine.engine import getEngine, ANY_ADAPTOR

from saga.constants import *


class Context (saga.Attributes) :

    def __init__  (self, type=None, _adaptor=None, _adaptor_state={}) :
        '''
        type: string
        ret:  None
        '''

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        self._attributes_register  (TYPE,            None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (SERVER,          None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (CERT_REPOSITORY, None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (USER_PROXY,      None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (USER_CERT,       None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (USER_KEY,        None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (USER_ID,         None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (USER_PASS,       None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (USER_VO,         None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (LIFE_TIME,       -1,   self.INT,    self.SCALAR, self.WRITABLE)
        self._attributes_register  (REMOTE_ID,       None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (REMOTE_HOST,     None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (REMOTE_PORT,     None, self.STRING, self.VECTOR, self.WRITABLE)
     
        self._logger = getLogger ('saga.Context')
        self._logger.debug ("saga.Context.__init__(%s)" % type)

        self._engine = getEngine ()

        if _adaptor :
            # created from adaptor
            self._adaptor = _adaptor
        else :
            # create from API -- create and bind adaptor
            self._adaptor = self._engine.bind_adaptor (self, 'saga.Context', type,
                                                       NOTASK, ANY_ADAPTOR, type)

    def _initialize (self, session) :
        '''
        ret:  None
        '''
        return self._adaptor._initialize (session)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

