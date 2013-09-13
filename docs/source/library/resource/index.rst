
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

Resource Manager -- :class:`saga.resource.Manager`
--------------------------------------------------

.. autoclass:: saga.resource.Manager
   :members:
   :undoc-members:
   :show-inheritance:


.. _resource_description:

Resource Description -- :class:`saga.resource.Description`
----------------------------------------------------------

.. autoclass:: saga.resource.Description
   :members:
   :undoc-members:


.. _resource_resource:

Resource -- :class:`saga.resource.Resource`
-------------------------------------------

.. autoclass:: saga.resource.Resource
   :members:
   :undoc-members:
