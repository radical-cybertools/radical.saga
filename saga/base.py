
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import string

import saga.utils.logger
import saga.engine.engine
import saga.adaptors.base      as sab
import saga.utils.signatures   as sus

# ------------------------------------------------------------------------------
#
class SimpleBase (object) :
    """ This is a very simple API base class which just initializes
    the self._logger and self._engine members, but does not perform any further
    initialization, nor any adaptor binding.  This base is used for API classes
    which are not backed by multiple adaptors (no session, tasks, etc).
    """

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('SimpleBase')
    @sus.returns (sus.nothing)
    def __init__  (self) :

        self._apitype   = self._get_apitype ()
        self._engine    = saga.engine.engine.Engine ()
        self._logger    = saga.utils.logger.getLogger (self._apitype)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('SimpleBase')
    @sus.returns (basestring)
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

    
# ------------------------------------------------------------------------------
#
class Base (SimpleBase) :

    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Base',
                  basestring, 
                  sus.optional (sab.Base), 
                  sus.optional (dict), 
                  sus.optional (sus.anything),
                  sus.optional (sus.anything))
    @sus.returns (sus.nothing)
    def __init__  (self, schema, adaptor, adaptor_state, *args, **kwargs) :

        SimpleBase.__init__ (self)

        self._adaptor = adaptor
        self._adaptor = self._engine.bind_adaptor   (self, self._apitype, schema, adaptor)


        # Sync creation (normal __init__) will simply call the adaptor's
        # init_instance at this point.  _init_task should *not* be evaluated,
        # ever, for __init__ based construction! (it is private, see?)
        #
        # For any async creation (create()), we need to return a task which
        # performs initialization.  We rely on the sync/async method decorators
        # on CPI level to provide the task instance itself, and point the task's
        # workload to the adaptor level init_instance method.
        self._init_task = self._adaptor.init_instance (adaptor_state, *args, **kwargs)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Base')
    @sus.returns ('saga.Session')
    def get_session (self) :
        """ 
        Returns the session which is managing the object instance.  For objects
        which do not accept a session handle on construction, this call returns
        None.

        The object's session is also available via the `session` property.
        """
        return self._adaptor.get_session ()

    session = property (get_session)

#
# ------------------------------------------------------------------------------


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

