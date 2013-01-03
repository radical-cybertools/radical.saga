
import string

from saga.engine.logger import getLogger
from saga.engine.engine import getEngine

class Base (object) :

    def __init__  (self, schema, adaptor, adaptor_state, *args, **kwargs) :

        ctype = self._get_ctype ()

        self._logger    = getLogger (ctype)
        self._logger.debug ("[saga.Base] %s.__init__()" % ctype)

        self._engine    = getEngine ()
        self._adaptor   = adaptor
        self._adaptor   = self._engine.bind_adaptor   (self, ctype, schema, adaptor)

        print " state  : %s " % str(adaptor_state)
        print " args   : %s " % str(args)
        print " kwargs : %s " % str(kwargs)

        self._init_task = self._adaptor.init_instance (adaptor_state, *args, **kwargs)



    def _get_ctype (self) :

        ctype = self.__module__ + '.' + self.__class__.__name__

        name_parts = ctype.split ('.')
        l = len(name_parts)

        if len > 2 :
          t1 = name_parts [l-1]
          t2 = name_parts [l-2]

          if t1 == string.capwords (t2) :
              del name_parts[l-2]

          ctype = string.join (name_parts, '.')

        return ctype


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

