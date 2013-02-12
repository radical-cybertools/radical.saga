
*********************
saga.adaptor.localjob
*********************

 
A more elaborate description....


Version
=======

v0.1


Supported Schemas
=================

  - **fork** : desc
  - **local** : same as fork



Configuration Options
=====================

``enabled``

enable / disable saga.adaptor.localjob adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]


Supported Capabilities
======================

``Supported Monitorable Metrics``

  - State

``Supported Job Attributes``

  - ExitCode
  - ExecutionHosts
  - Created
  - Started
  - Finished

``Supported Context Types``

  - *None*: this adaptor works in the same security
                                      context as the application process itself.

``Supported Job Description Attributes``

  - Executable
  - Arguments
  - Environment
  - WorkingDirectory
  - Input
  - Output
  - Error
  - SPMDVariation
  - NumberOfProcesses



Supported API Classes
=====================

  - :class:`saga.job.Service`
  - :class:`saga.job.Job`


saga.job.Service
""""""""""""""""

.. autoclass:: saga.adaptors.local.localjob.LocalJobService
   :members:


saga.job.Job
""""""""""""

.. autoclass:: saga.adaptors.local.localjob.LocalJob
   :members:



