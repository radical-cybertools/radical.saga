
###################
saga.adaptor.sgejob
###################

Description
-----------

The SGE (Sun/Oracle Grid Engine) adaptor allows to run and manage jobs on
`SGE <http://en.wikipedia.org/wiki/Oracle_Grid_Engine>`_ controlled HPC clusters.



Supported Schemas
-----------------

This adaptor is triggered by the following URL schemes:

======================== ============================================================
schema                   description                                                 
======================== ============================================================
**sge://**               connect to a local cluster                                  
**sge+ssh://**           conenct to a remote cluster via SSH                         
**sge+gsissh://**        connect to a remote cluster via GSISSH                      
======================== ============================================================



Example
-------

.. literalinclude:: ../../../examples/jobs/sgejob.py


Configuration Options
---------------------
Configuration options can be used to control the adaptor's runt    ime behavior. Most adaptors don't need any configuration options to b    e set in order to work. They are mostly for controlling experimental feat    ures and properties of the adaptors.

     .. s    eealso:: More information about configuration options can be found in the     :ref:`conf_file` section.

enabled
*******

enable / disable saga.adaptor.sgejob adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]


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

========================= ============================================================
                Attribute Description
========================= ============================================================
                     x509 GSISSH X509 proxy context
                 userpass username/password pair (ssh)
                      ssh SSH public/private keypair
========================= ============================================================

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
  - WallTimeLimit
  - WorkingDirectory
  - SPMDVariation
  - TotalCPUCount



Supported API Classes
---------------------

This adaptor supports the following API classes:
  - :class:`saga.job.Service`
  - :class:`saga.job.Job`

Method implementation details are listed below.

saga.job.Service
****************

.. autoclass:: saga.adaptors.sge.sgejob.SGEJobService
   :members:


saga.job.Job
************

.. autoclass:: saga.adaptors.sge.sgejob.SGEJob
   :members:



