
############################
Writing SAGA-Python Adaptors
############################

.. note::

   This part of the SAGA-Python documentation is not for *users* of SAGA-Python,
   but rather for implementors of backend adaptors (although it may be
   beneficial for users to skim over section :ref:`adaptor_binding` to gain
   some insight into SAGA-Python's  mode of operation).



.. _adaptor_structure:

Adaptor Structure
-----------------

A SAGA-Python adaptor is a Python module with well defined structure.  The
module must expose a class ``Adaptor``, which (a) must be a singleton, (b) must
provide a ``sanity_check()`` method, and (c) must inherit from
:class:`saga.cpi.base.AdaptorBase`.  That base class' constructor (``__init__``)
must be called like this::


  class Adaptor (saga.cpi.base.AdaptorBase):

    __metaclass__ = Singleton

    def __init__ (self) :
      saga.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)


    def sanity_check (self) :
      # FIXME: detect gsissh tool
      pass




``_ADAPTOR_INFO`` and ``_ADAPTOR_OPTIONS`` are Python ``dict``\ s with the
following layout::


  _ADAPTOR_NAME          = 'saga.adaptor.gsissh.job'
  _ADAPTOR_SCHEMAS       = ['ssh', 'gsissh']
  _ADAPTOR_OPTIONS       = [
    { 
    'category'         : _ADAPTOR_NAME, 
    'name'             : 'cache_connection', 
    'type'             : bool, 
    'default'          : True,
    'valid_options'    : [True, False],
    'documentation'    : 'toggle connection caching.',
    'env_variable'     : None
    },
  ]
  _ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.1',
    'cpis'             : [
      { 
      'type'         : 'saga.job.Service',
      'class'        : 'GSISSHJobService',
      'schemas'      : _ADAPTOR_SCHEMAS
      }, 
      { 
      'type'         : 'saga.job.Job',
      'class'        : 'GSISSHJob',
      'schemas'      : _ADAPTOR_SCHEMAS
      }
    ]
  }



(It is beneficial to specify ``_ADAPTOR_NAME`` and ``_ADAPTOR_SCHEMAS``
separately, as they are used in multiple places, as explained later on.)

The *adaptor classes* listed in the ``_ADAPTOR_INFO`` (in this example,
``GSISSHJob`` and ``GSISSHJobService``) are the classes which are actually bound
to a SAGA API object, and provide its functionality.  For that purpose, those
classes must inherit the respective object's Capability Provider Interface
(CPI), as shown in the stub below::

  class GSISSHJobService (saga.cpi.job.Service) :

    def __init__ (self, api, adaptor) :
      saga.cpi.Base.__init__ (self, api, adaptor, 'GSISSHJobService')
  
  
    @SYNC
    def init_instance (self, rm_url, session) :
      self._rm      = rm_url
      self._session = session
  
  
The :class:`saga.cpi.Base` class will make sure that the adaptor classes keep
a ``self._adaptor`` member, pointing to the adaptor singleton instance (i.e. the
module's ``Adaptor`` class instance).  It will further initialize a logging module
(available as ``self._logger``).

Note that the adaptor class\' ``__init__`` does not correspond to the API level
object ``__init__`` -- instead, the adaptor class construction is a two step
process, and the actual constructor semantics is mapped to an
``init_instance()`` method, which receives the API level constructor arguments.





.. _adaptor_registration:

Adaptor Registration
--------------------

Any SAGA adaptor must be registered in the :ref:``Engine`` in order to be
usable.  That process is very simple, and performed by the
:class:`saga.cpi.base.AdaptorBase` class -- so all the adaptor has to take care
of is the correct initialization of that base class, as described in
:ref:`adaptor_structure`.  The ``AdaptorBase`` will forward the
``_ADAPTOR_INFO`` to the :class:`saga.engine.Engine` class, where the adaptor
will be added to a registry of adaptor classes, hierarchically sorted like
this (simplified)::

  Engine._adaptor_registry = 
  { 
    'saga.job.Service' : 
    { 
      'gshiss' : [saga.adaptors.gsissh.job.GSISSHJobService, ...]
      'ssh'    : [saga.adaptors.gsissh.job.GSISSHJobService, ...]
      'gram'   : [saga.adaptors.globus.job.GRAMJobService, ...]
      ...
    },
    'saga.job.Job' : 
    { 
      'gshiss' : [saga.adaptors.gsissh.job.GSISSHJob, ...]
      'ssh'    : [saga.adaptors.gsissh.job.GSISSHJob, ...]
      'gram'   : [saga.adaptors.globus.job.GRAMJob, ...]
      ...
    },
    ...
  }

That registry is searched when the engine binds an adaptor class instance to
a SAGA API object instance -- see :ref:`adaptor_binding`.



.. _adaptor_binding:

Adaptor Binding
---------------

Whenever a SAGA API object is created, or whenever any method is invoked on that
object, the SAGA-Python implementation needs to (a) select a suitable backend
adaptor to perform that operation, and (b) invoke the respective adaptor
functionality.  

The first part, selecting a specific adaptor for a specific API
object instance, is called *binding* -- SAGA-Python binds an adaptor to an
object exactly once, at creation time, and the bond remains valid for the
lifetime of the API object: on API creation, the API object will request
a suitable adaptor from the engine, and will keep it for further method
invocations (code simplified)::

  class Service (object) :
  
    def __init__ (self, url=None, session=None) : 
      self._engine  = getEngine ()
      self._adaptor = self._engine.get_adaptor (self, 'saga.job.Service', url.scheme, ...,
                                                  url, session)

    def run_job (self, cmd, host="", ttype=None) :
      return self._adaptor.run_job (cmd, host, ttype=ttype)
    
    ...


The ``Engine.get_adaptor`` call will iterate through the engine's adaptor
registry, and will, for all adaptors which indicated support for the given URL
scheme, request an adaptor class instance for the given API class.  If an
adaptor class instance can successfully be created, the engine will further
attempt to call the adaptor class' ``init_instance`` method, which will in fact
construct an adaptor level representation of the API level object::

  class GSISSHJobService (saga.cpi.job.Service) :

    def __init__ (self, api, adaptor) :
      saga.cpi.Base.__init__ (self, api, adaptor, 'GSISSHJobService')

    def init_instance (self, url, session) :
      # - check if session contains suitable security tokens for (gsi)ssh
      # - check if endpoint given by 'url' can be served
      # - establish and cache connection to that endpoint, with the sesssion
      #   credentials
      ...


If either the adaptor class instantiation or the ``init_instance`` invocation
raise an exception, the engine will try the next registered adaptor for that API
class / url scheme combo.  If both steps are successful, the adaptor class
instance will be returned to the API object's constructor, as shown above.



.. _adaptor_state:

Adaptor State
-------------

Instances of adaptor classes will often need to share some state.  For example,
different instances of ``saga.job.Job`` running via ssh on a specific host may
want to share a single ssh connection; asynchronous operations on a specific
adaptor may want to share a thread pool; adaptor class instances of a specific
resource adaptor may want to share a notification endpoint.  State sharing
supports scalability, and can simplify adaptor code -- but also adds some
overhead to exchange and synchronize state between those adaptor class
instances.

The preferred way to share state is to use the adaptor instance (as it was
created by the engine while loading the adaptor's module) for state exchange
(see section :ref:`adaptor_registration` -- all adaptor class instances get the
spawning adaptor instance passed at creation time::


  class GSISSHJobService (saga.cpi.job.Service) :

    def __init__ (self, api, adaptor) :
      saga.cpi.Base.__init__ (self, api, adaptor, 'GSISSHJobService')


:class:`saga.cpi.Base` will make that instance available as ``self._adaptor``.
As that adaptor class is part of the adaptor modules code base, and thus under
full control of the adaptor developer, it is straight forward to use it for
state caching and state exchange.  Based on the example code in section
:ref:`adaptor_structure`, a connection caching adaptor class could look like
this::

  class Adaptor (saga.cpi.base.AdaptorBase):

    __metaclass__ = Singleton

    def __init__ (self) :
      saga.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)
      self._cache = {}
      ...
    ...


  class GSISSHJobService (saga.cpi.job.Service) :

    def __init__ (self, api, adaptor) :
      ...
      saga.cpi.Base.__init__ (self, api, adaptor, 'GSISSHJobService')
      self._cache = self._adaptor._cache
  
  
    @SYNC
    def init_instance (self, rm_url, session) :
      ...
      if not self._rm in self._adaptor.keys () :
        self._cache [self._rm] = setup_connection (self._rm)


    @SYNC
    def run_job (self, cmd) :
      ...
      connection = self._cache [self._rm]
      return connection.run (cmd)
    ...

            
The adaptor implementor is responsible for the consistency of the shared state,
and may need to use locking to ensure proper consistency in multithreaded
environments -- the ``self._adaptor`` class merely provides a shared container
for the data, nothing else.  Also, the Adaptor class\' destructor should take
care of freeing the cached / shared state objects (unless another cleanup
mechanism is in place).



.. _adaptor_apicreate:

Creating API objects on Adaptor Level
-------------------------------------



.. _adaptor_async:

Synchronous versus Asynchronous Adaptor Methods
-----------------------------------------------



.. _adaptor_exceptions:

Adaptor Level Exception Handling
--------------------------------



Module saga.engine
------------------

The config module provides classes and functions to introspect and modify
SAGA's configuration. The :func:`getConfig` function is used to get the
:class:`GlobalConfig` object which represents the current configuration 
of SAGA:

.. automodule:: saga.engine
   :members:    saga.engine.Engine


