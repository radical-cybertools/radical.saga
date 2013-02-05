
**************************
saga.adaptor.replica.irods
**************************

This adaptor interacts with the iRODS data
management system, by using the iRODS command line
tools.

Version
=======

v0.1


Supported Schemas
=================

  - **irods** : irods schema



Configuration Options
=====================

``enabled``

enable / disable saga.adaptor.replica.irods adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]


Supported Capabilities
======================



Supported API Classes
=====================

  - :class:`saga.replica.LogicalDirectory`
  - :class:`saga.replica.LogicalFile`


saga.replica.LogicalDirectory
"""""""""""""""""""""""""""""

.. autoclass:: saga.adaptors.irods.irods_replica.IRODSDirectory
   :members:


saga.replica.LogicalFile
""""""""""""""""""""""""

.. autoclass:: saga.adaptors.irods.irods_replica.IRODSFile
   :members:



