
##########################
saga.adaptor.replica.irods
##########################

Description
-----------
The iRODS replica adaptor.


Supported Schemas
-----------------

This adaptor is triggered by the following URL schemes:

======================== ============================================================
schema                   description                                                 
======================== ============================================================
**irods://**             irods schema                                                
======================== ============================================================



Example
-------

.. literalinclude:: ../../../examples/replica/irods/irods_test.py


Configuration Options
---------------------
Configuration options can be used to control the adaptor's     runtime behavior. Most adaptors don't need any configuration options     to be set in order to work. They are mostly for controlling experimental     features and properties of the adaptors.

     .. seealso:: More information about configuration options can be found in     the :ref:`conf_file` section.

enabled
*******

enable / disable saga.adaptor.replica.irods adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]



Capabilities
------------



Supported API Classes
---------------------

This adaptor supports the following API classes:
  - :class:`saga.replica.LogicalDirectory`
  - :class:`saga.replica.LogicalFile`

Method implementation details are listed below.

saga.replica.LogicalDirectory
*****************************

.. autoclass:: saga.adaptors.irods.irods_replica.IRODSDirectory
   :members:


saga.replica.LogicalFile
************************

.. autoclass:: saga.adaptors.irods.irods_replica.IRODSFile
   :members:



