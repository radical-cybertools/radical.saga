
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



It is beneficial to specify ``_ADAPTOR_NAME`` and ``_ADAPTOR_SCHEMAS``
separately, as they are used in multiple places, as explained later on.

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
          """ Service instance constructor """
  
          self._rm      = rm_url
          self._session = session
  
  
The :class:`saga.cpi.Base` class will make sure that the adaptor classes keep
a ``self._adaptor`` member, pointing to the adaptor singleton instance (i.e. the
module's ``Adaptor`` class instance).  It will further initialize a logging module
(available as ``self._logger``).

Note that the adaptor class' ``__init__`` does not correspond to the API level
object ``__init__`` -- instead, the adaptor class construction is a two step
process, and the actual constructor semantics is mapped to an ``init_instance``()
method, which receives the API level constructor arguments.





.. _adaptor_registration:

Adaptor Registration
--------------------

Any SAGA adaptor must be registered in the :ref:``Engine`` in order to be usable.
That process involves several steps.



.. _adaptor_binding:

Adaptor Binding
---------------



.. _adaptor_state:

Adaptor State
-------------



Module saga.engine
------------------

The config module provides classes and functions to introspect and modify
SAGA's configuration. The :func:`getConfig` function is used to get the
:class:`GlobalConfig` object which represents the current configuration 
of SAGA:

.. automodule:: saga.engine
   :members:    saga.engine.Engine


