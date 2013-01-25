
.. _chapter_adaptor_writing

***************************
Writing SAGA-Python Adaptors
***************************

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
:class:`saga.cpi.AdaptorBase`.  That base class' constructor (``__init__``)
must be called like this::


  class Adaptor (saga.cpi.AdaptorBase):

    __metaclass__ = Singleton

    def __init__ (self) :
      saga.cpi.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)


    def sanity_check (self) :
      # FIXME: detect gsissh tool
      pass




``_ADAPTOR_INFO`` and ``_ADAPTOR_OPTIONS`` are module level Python :class:`dict`\ s with the
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
    'documentation'    : 'toggle connection caching',
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
:class:`saga.cpi.AdaptorBase` class -- so all the adaptor has to take care
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
      saga.cpi.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)
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

Creating API Objects
--------------------

Several SAGA API objects feature factory-like methods -- examples are
``Directory.open()``, ``job.Service.create_job()/run_job()``, and
``resource.Manager.aquire()``.  To correctly implement those methods on adaptor
level, adaptors need to be able to instantiate the API objects to return.  We
have seen in section :ref:`adaptor_binding` that, on API object creation, the
``Engine`` will select and bind a suitable adaptor to the object instance.  In
many cases, however, an implementation of a factory-like API method will want to
make sure that the resulting API object is bound to the same adaptor instance as
the spawning adaptor class instance itself.  For that purpose, all API object
constructors will accept two additional parameters: ``_adaptor`` (type:
:class:`saga.cpi.Base` or derivative), and ``_adaptor_state`` (type:
:class:`dict`).  This is also provided for API objects which normally have no
public constructor at all::


  class Job (saga.attributes.Attributes, saga.task.Async) :
      
    def __init__ (self, _adaptor=None, _adaptor_state={}) :
  
      if not _adaptor :
          raise saga.exceptions.IncorrectState ("saga.job.Job constructor is private")
      ...
    
      # bind to the given adaptor -- this will create the required adaptor
      # class.  We need to specify a schema for adaptor selection -- and
      # simply choose the first one the adaptor offers.
      engine         = getEngine ()
      adaptor_schema = _adaptor.get_schemas()[0]
      self._adaptor  = engine.bind_adaptor (self, 'saga.job.Job', adaptor_schema, 
                                            saga.task.NOTASK, _adaptor, _adaptor_state)


As shown above, ``_adaptor`` and ``_adaptor_state`` are forwarded to the
Engine\'s ``bind_adaptor()`` method, and if present will ensure that the
resulting API object is bound to the given adaptor.  The ``_adaptor_state`` dict
will be forwarded to the adaptor class level ``init_instance()`` call, and can
be used to correctly initialize the state of the new adaptor class.  An example
of adaptor level code for creating an :class:`saga.job.Job` instance via
:class:`saga.job.Service`\ ``.create_job()`` is below::

  class GSISSHJobService (saga.cpi.job.Service) :
      
    def __init__ (self, api, adaptor) :
        saga.cpi.Base.__init__ (self, api, adaptor, 'GSISSHJobService')
  
    @SYNC
    def init_instance (self, rm_url, session) :
      ...
  
    @SYNC
    def create_job (self, jd) :
      
      state = { 'job_service'     : self, 
                'job_description' : jd, 
                'session'         : self._session}
  
      return saga.job.Job (_adaptor=self._adaptor, _adaptor_state=state)
  
  
  class GSISSHJob (saga.cpi.job.Job) :
    def __init__ (self, api, adaptor) :
      saga.cpi.Base.__init__ (self, api, adaptor, 'GSISSHJob')
      ...
  
    @SYNC
    def init_instance (self, job_info):

      self._session        = job_info['session']
      self._jd             = job_info['job_description']
      self._parent_service = job_info['job_service'] 
  
      self._id             = None # is assigned when calling job.run()
      self._state          = saga.job.NEW
  
      # register ourselves with the parent service
      self._parent_service._update_jobid (self, self._id)
      ...
  
  

.. _adaptor_exceptions:

Exception Handling
------------------

SAGA-Python defines a set of exceptions which can be thrown on the various
method invocations (see section :ref:`api_exceptions`.  Adaptor implementors
must ensure, that the correct exception types are thrown on the corresponding
error conditions.  If the API layer encounters a non-SAGA exception from the
adaptor layer, it will convert it to a ``saga.NoSuccess`` exception.  While that
will reliably shield the user layer from non-SAGA exception types, it is a lossy
translation, and likely to hide the underlying cause of the error.  This feature
is thus to be considered as a safe guard, not as a preferred method of error
state communication!

An example of adaptor level error handling is below::

  class ContextX509 (saga.cpi.Context) :
  
    def __init__ (self, api, adaptor) :
      saga.cpi.Base.__init__ (self, api, adaptor, 'ContextX509')
  
    @SYNC
    def init_instance (self, type) :
      if not type.lower () in (schema.lower() for schema in _ADAPTOR_SCHEMAS) :
        raise saga.exceptions.BadParameter \
                ("the x509 context adaptor only handles x509 contexts - duh!")
  
    @SYNC
    def _initialize (self, session) :
  
      if not self._api.user_proxy :
        self._api.user_proxy = "x509up_u%d"  %  os.getuid()   # fallback

      if not os.path.exists (self._api.user_proxy) or \
         not os.path.isfile (self._api.user_proxy)    :
        raise saga.exceptions.BadParameter ("X509 proxy does not exist: %s"
                                                 % self._api.user_proxy)
  
      try :
        fh = open (self._api.user_proxy)
      except Exception as e:
        raise saga.exceptions.PermissionDenied ("X509 proxy '%s' not readable: %s"
                                             % (self._api.user_proxy, str(e)))
      else :
        fh.close ()
  



.. _adaptor_async:

Asynchronous Methods
--------------------

The SAGA API features several objects which implement both synchronous and
asynchronous versions of their respective methods.  Synchronous calls will
return normal objects or values; asynchronous calls will return
:class:`saga.Task` instances, which represent the ongoing asynchronous method,
and can later be inspected for state and return values.

On adaptor level, both method types are differences by the method decorators
``@SYNC`` and ``@ASYNC``, like this::

  class LocalFile (saga.cpi.filesystem.File) :
  
    def __init__ (self, api, adaptor) :
        saga.cpi.Base.__init__ (self, api, adaptor, 'LocalFile')
  
    @SYNC
    def init_instance (self, url, flags, session) :
        self._url     = url
        self._flags   = flags
        self._session = session
        ...
  
  
    @ASYNC
    def init_instance_async (self, ttype, url, flags, session) :
      self._url     = url
      self._flags   = flags
      self._session = session

      t = saga.task.Task ()
      t._set_result (saga.filesystem.File (url, flags, session, _adaptor_name=_ADAPTOR_NAME))
      t._set_state  (saga.task.DONE)

      return t


    @SYNC
    def get_url (self) :
      return self._url
  
    @ASYNC
    def get_url_async (self, ttype) :
  
      t = saga.task.Task ()
      t._set_result (self._url)
      t._set_state  (saga.task.DONE)
  
      return t


Note that the async calls in the example code above are not *really*
asynchronous, as they both return a task which is in ``Done`` state -- a proper
async call would return a task in ``New`` or ``Running`` state, without setting
the task\'s result, and would perform some required work in a separate thread or
process.  Upon completion, the adaptor (which should keep a handle on the
created task) would then set the result and state of the task, thus notifying
the application of the completion of the asynchronous method call.

Also note that there exists an asynchronous version for the ``init_instance()``
method, which is used for the asynchronous API object creation, i.e. on::

  #import sys
  #import saga

  t = saga.job.Service.create ('ssh://host.net')
  
  t.wait ()

  if t.state != saga.task.DONE :
    print "no job service: " + str(t.exception)
    sys.exit (0)

  job_service = t.get_result ()
  job_service.run_job ("touch /tmp/hello_world")


The exact semantics of SAGA's asynchronous operations is described elsewhere
(see section :ref:`api_tasks`).  Relevant for this discussion is to note that
the asynchronous adaptor methods all receive a task type parameter (``ttype``)
which determines the state of the task to return: on ``ttype==saga.task.TASK``,
the returned task should be in ``New`` state; on ``ttype==saga.task.ASYNC`` the
task is in ``Running`` state, and on ``ttype==saga.task.SYNC`` the returned task
is already ``Done``.  It is up to the adaptor implementor to ensure that
semantics -- the examples above do not follow it, and are thus incomplete.



.. _adaptor_bulks:

Bulk Operations:
----------------

On API level, there exists no explicit support for bulk operations.  Those can,
however, be rendered implicitly, by collecting asynchronous operations in a task
container, and calling ``run()`` on that container::

  bulk  = saga.task.Container ()

  dir_1 = saga.filesystem.Directory ("gridftp://remote.host1.net/")
  for i in ranger (0, 1000) :

    src = "gridftp://remote.host1.net/data/file_%4d.dat"  %  i
    tgt = "gridftp://other.hostx.net/share/file_%4d.dat"  %  i

    bulk.add (dir1.copy (src, tgt, saga.task.TASK))


  dir_2 = saga.filesystem.Directory ("ssh://remote.host2.net/")
  for i in ranger (0, 1000) :

    src = "ssh://remote.host2.net/data/file_%4d.info"  %  i
    tgt = "ssh://other.hostx.net/share/file_%4d.info"  %  i

    bulk.add (dir2.copy (src, tgt, saga.task.TASK))


  bulk.run  ()
  bulk.wait (saga.task.ALL)


The above task container gets filled by file copy tasks which are addressing two
different file transfer protocols, and are thus likely mapped to two different
adaptors.  The SAGA-Python API implementation will inspect the task container
upon ``bulk.run()``, and will attempt to sort the contained tasks by adaptor,
i.e. all tasks operating on API objects which bind to the same adaptor instance
(not adaptor class instance) will be lumped together into a task *bucket*.  For
each bucket, the API will then call the respective bulk operation
(``container_method``) for that adaptor.

Note that at this point, the task container implementation does not yet know
what *adaptor class* instance to use for the bulk operations.  For that purpose,
it will inspect

:todo: Needs completion after generic bulk ops are fixed.

    


.. _adaptor_logging:

Adaptor Logging
---------------

Based on Python\'s ``logging`` facility, SAGA-Python also supports logging, both
as an internal auditing and debugging mechanism, and as application supporting
capability (see section :ref:`util_logging`.  Adaptor implementors are
encouraged to use logging as well -- for that purposed, the
:class:`saga.cpi.AdaptorBase` and :class:`saga.cpi.Base` classes will initialize
a ``self._logger`` member for all adaptor and adaptor class implementations,
respectively.

We advice to use the log levels as indicated below:

   +---------------------+------------------------------------+
   | Log Level           | Type of Events Logged              |
   +=====================+====================================+
   | ``CRITICAL``        | Only fatal events that will cause  |
   |                     | the process to abort -- that       |
   |                     | NEVER happen on adaptor level!     |
   +---------------------+------------------------------------+
   | ``ERROR``           | Events that will prevent the       |
   |                     | adaptor from functioning correctly.|
   +---------------------+------------------------------------+
   | ``WARNING``         | Events that indicate potential     |
   |                     | problems or unusual events, and    |
   |                     | can support application            |
   |                     | diagnostics.                       |
   +---------------------+------------------------------------+
   | ``INFO``            | Events that support adaptor        |
   |                     | auditing, inform about backend     |
   |                     | activities, performance etc.       |
   +---------------------+------------------------------------+
   | ``DEBUG``           | Events which support the tracking  |
   |                     | of adaptor code paths, to support  |
   |                     | adaptor debugging (lots of output).|
   +---------------------+------------------------------------+


.. _adaptor_process:

External Processes
------------------

For many adaptor implementations, it is beneficial to interface to external
processes.  In particular, all adaptors performing remote interactions over ssh
or gsissh secured connections will likely need to interact with the remote user
shell.  The :class:`saga.utils.pty_process` class is designed to simplify those
interactions, and at the same time be more performant than, for example,
pexpect.


.. autoclass:: saga.utils.pty_process

