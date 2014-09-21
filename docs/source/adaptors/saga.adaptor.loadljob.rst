
#####################
saga.adaptor.loadljob
#####################

Description
-----------

The LoadLeveler adaptor allows to run and manage jobs on ` IBM LoadLeveler<http://www-03.ibm.com/systems/software/loadleveler/>`_
controlled HPC clusters.



Supported Schemas
-----------------

This adaptor is triggered by the following URL schemes:

======================== ============================================================
schema                   description                                                 
======================== ============================================================
**loadl://**             connect to a local cluster                                  
**loadl+ssh://**         conenct to a remote cluster via SSH                         
**loadl+gsissh://**      connect to a remote cluster via GSISSH                      
======================== ============================================================



Example
-------

.. literalinclude:: ../../../examples/jobs/loadljob.py


Configuration Options
---------------------
Configuration options can be used to control the adaptor's     runtime behavior. Most adaptors don't need any configuration options     to be set in order to work. They are mostly for controlling experimental     features and properties of the adaptors.

     .. seealso:: More information about configuration options can be found in     the :ref:`conf_file` section.

enabled
*******

enable / disable saga.adaptor.loadljob adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]

purge_on_start
**************

Purge temporary job information for all
jobs which are older than a number of days.
The number of days can be configured with <purge_older_than>.


  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]

purge_older_than
****************

When <purge_on_start> is enabled this specifies the number
of days to consider a temporary file older enough to be deleted.


  - **type** : <type 'int'>
  - **default** : 30
  - **environment** : None



Capabilities
------------

Supported Monitorable Metrics
*****************************

  - State

Supported Job Attributes
************************

  - ExitCode
  - ExecutionHosts
  - Created
  - Started
  - Finished

Supported Context Types
***********************

============================================================ ============================================================
                                                   Attribute Description
============================================================ ============================================================
                             :ref:`security_contexts` : x509 GSISSH X509 proxy context
                         :ref:`security_contexts` : userpass username/password pair (ssh)
                              :ref:`security_contexts` : ssh SSH public/private keypair
============================================================ ============================================================

Supported Job Description Attributes
************************************

  - Name
  - Executable
  - Arguments
  - Environment
  - Input
  - Output
  - Error
  - Queue
  - Project
  - JobContact
  - WallTimeLimit
  - WorkingDirectory
  - TotalPhysicalMemory
  - TotalCPUCount



Supported API Classes
---------------------

This adaptor supports the following API classes:
  - :class:`saga.job.Service`
  - :class:`saga.job.Job`

Method implementation details are listed below.

saga.job.Service
****************

.. autoclass:: saga.adaptors.loadl.loadljob.LOADLJobService
   :members:


saga.job.Job
************

.. autoclass:: saga.adaptors.loadl.loadljob.LOADLJob
   :members:



