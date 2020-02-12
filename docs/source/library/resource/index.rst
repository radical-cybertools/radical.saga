
.. _resource_management:

Resource Management
===================

Let's start with a basic example. We start a VM on Amazon EC2 using the
SAGA resource API, submit a job to the newly instantiated VM using the
SAGA job API and finally shut down the VM.

.. note:: In order to run this example, you need an account with Amazon EC2. You 
          also need your Amazon EC2 id and key. 

.. literalinclude:: ../../../../examples/resource/amazon_ec2.py


.. contents:: Table of Contents
   :local:

.. _resource_manager:

Resource Manager -- :class:`radical.saga.resource.Manager`
--------------------------------------------------

.. autoclass:: radical.saga.resource.Manager
   :members:
   :undoc-members:
   :show-inheritance:
   :special-members: __init__
   :exclude-members: create, __module__


.. _resource_description:

Resource Description -- :class:`radical.saga.resource.Description`
----------------------------------------------------------

.. autoclass:: radical.saga.resource.Description
   :members:
   :undoc-members:
   :special-members: __init__
   :exclude-members: create, __module__, __deepcopy__


.. _resource_resource:

Resource -- :class:`radical.saga.resource.Resource`
-------------------------------------------

.. autoclass:: radical.saga.resource.Resource
   :members:
   :undoc-members:
   :exclude-members: create, __module__


Compute  Resource -- :class:`radical.saga.resource.Compute`
---------------------------------------------------

.. autoclass:: radical.saga.resource.Compute
   :members:
   :show-inheritance:

Storage  Resource -- :class:`radical.saga.resource.Storage`
---------------------------------------------------

.. autoclass:: radical.saga.resource.Storage
   :members:
   :show-inheritance:

Storage  Resource -- :class:`radical.saga.resource.Network`
---------------------------------------------------

.. autoclass:: radical.saga.resource.Network
   :members:
   :show-inheritance:
