.. _replica_management:

Replica Management
==================

SAGA's replica management module is ...

The basic usage of the replica module is as follows::

   # ...
   ...

.. seealso:: More examples on how to use the SAGA replica module can be found in 
             the :ref:`code_examples_replica` section of the 
             :ref:`chapter_code_examples` chapter.

Like all SAGA modules, the replica module relies on  middleware adaptors 
to provide bindings to a specific resource manager. Adaptors are implicitly 
selected via the `scheme` part of the URL, e.g., ``local://`` in the example 
above selects the `local` replica adaptor.

.. note:: A list of available adaptors and supported resource managers can be 
          found in the :ref:`chapter_middleware_adaptors` part of this 
          documentation.

The rest of this section is structured as follows:

.. contents:: Table of Contents
   :local:

.. #############################################################################
.. _replica_file:

Replica Service -- :class:`saga.replica.LogicalFile`
----------------------------------------------------

:todo: Describe how to work with services.

.. autoclass:: saga.replica.LogicaLfile
   :members:
   :undoc-members:

