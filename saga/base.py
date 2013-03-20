
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import sys
import string
import inspect

import saga.utils.logger
import saga.engine.engine

class SimpleBase (object) :
    """ This is a very simple API base class which just initializes
    the self._logger and self._engine members, but does not perform any further
    initialization, nor any adaptor binding.  This base is used for API classes
    which are not backed by multiple adaptors (no session, tasks, etc).
    """

    def __init__  (self) :

        self._apitype   = self._get_apitype ()
        self._engine    = saga.engine.engine.Engine ()
        self._logger    = saga.utils.logger.getLogger (self._apitype)

      # self._logger.debug ("[saga.Base] %s.__init__()" % self._apitype)


    def _get_apitype (self) :

        # apitype for saga.job.service.Service should be saga.job.Service --
        # but we need to make sure that this actually exists and is equivalent.

        mname_1 = self.__module__           # saga.job.service
        cname   = self.__class__.__name__   # Service

        mname_1_elems = mname_1.split ('.') # ['saga', 'job', 'service']
        mname_1_elems.pop ()                # ['saga', 'job']
        mname_2 = '.'.join (mname_1_elems)  #  'saga.job'

        for mod_cname, mod_class in inspect.getmembers (sys.modules[mname_2]) :
            if  mod_cname == cname           and \
                inspect.isclass (mod_class)  and \
                isinstance (self, mod_class)     :

                apitype = "%s.%s" % (mname_2, cname) # saga.job.Service
                return apitype

        apitype = "%s.%s" % (mname_1, cname) # saga.job.service.Service
        return apitype


class Base (SimpleBase) :

    def __init__  (self, schema, adaptor, adaptor_state, *args, **kwargs) :

        SimpleBase.__init__ (self)

        self._adaptor = adaptor
        self._adaptor = self._engine.bind_adaptor (self, self._apitype, schema, adaptor)


        # Sync creation (normal __init__) will simply call the adaptor's
        # init_instance at this point.  _init_task should *not* be evaluated,
        # ever, for __init__ based construction! (it is private, see?)
        #
        # For any async creation (create()), we need to return a task which
        # performs initialization.  We rely on the sync/async method decorators
        # on CPI level to provide the task instance itself, and point the task's
        # workload to the adaptor level init_instance method.
        self._init_task = self._adaptor.init_instance (adaptor_state, *args, **kwargs)


    def get_session (self) :
        """ 
        Returns the session which is managing the object instance.  For objects
        which do not accept a session handle on construction, this call returns
        None.

        The object's session is also available via the `session` property.
        """
        return self._adaptor.get_session ()

    session = property (get_session)



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

