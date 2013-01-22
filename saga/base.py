
import string

from saga.engine.logger import getLogger
from saga.engine.engine import getEngine

class SimpleBase (object) :
    """ This is a very simple API base class which just initializes
    the self._logger and self._engine members, but does not perform any further
    initialization, nor any adaptor binding.  This base is used for API classes
    which are not backed by a (single) adaptor (session, task, etc).
    """

    def __init__  (self) :

        self._apitype   = self._get_apitype ()
        self._engine    = getEngine ()
        self._logger    = getLogger (self._apitype)
        self._logger.debug ("[saga.Base] %s.__init__()" % self._apitype)


    def _get_apitype (self) :

        apitype = self.__module__ + '.' + self.__class__.__name__

        name_parts = apitype.split ('.')
        l = len(name_parts)

        if len > 2 :
          t1 = name_parts [l-1]
          t2 = name_parts [l-2]
          t2 = t2.replace ('_', ' ')
          t2 = string.capwords (t2)
          t2 = t2.replace (' ', '')

          if t1 == t2 :
              del name_parts[l-2]

          apitype = string.join (name_parts, '.')

        return apitype

    

class Base (SimpleBase) :

    def __init__  (self, schema, adaptor, adaptor_state, *args, **kwargs) :

        SimpleBase.__init__ (self)

        self._adaptor = adaptor
        self._adaptor = self._engine.bind_adaptor   (self, self._apitype, schema, adaptor)

        self._init_task = self._adaptor.init_instance (adaptor_state, *args, **kwargs)



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

