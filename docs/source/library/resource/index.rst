
.. _resource_management:

Resource Management
===================

The basic usage of the resource module is as follows::

.. literalinclude:: ../../../examples/resource/amazon_ec2.py

.. seealso:: More examples on how to use the SAGA resource module can be found in 
             the :ref:`code_examples_resource` section of the 
             :ref:`chapter_code_examples` chapter.

Like all SAGA modules, the resource module relies on  middleware adaptors 
to provide bindings to a specific resource manager. Adaptors are implicitly 
selected via the `scheme` part of the URL, e.g., ``local://`` in the example 
above selects the `local` resource adaptor. 

.. note:: A list of available adaptors and supported resource managers can be 
          found in the :ref:`chapter_middleware_adaptors` part of this 
          documentation.


.. contents:: Table of Contents
   :local:

.. #############################################################################
.. _resource_manager:

Resource manager -- :class:`saga.resource.Manager`
--------------------------------------------------

:todo: Describe how to work with resource managers.

.. autoclass:: saga.resource.Manager
   :members:
   :undoc-members:

