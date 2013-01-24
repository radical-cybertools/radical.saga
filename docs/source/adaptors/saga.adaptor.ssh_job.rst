
********************
saga.adaptor.ssh_job
********************

 
A more elaborate description....


Version
=======

v0.1


Supported Schemas
=================

  - **gsissh** : use gsissh to run a remote job
  - **ssh** : use ssh to run a remote job



Configuration Options
=====================

``enabled``

enable / disable saga.adaptor.ssh_job adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]


Supported Capabilities
======================

``Supported Monitorable Metrics``

  - State
  - StateDetail

``Supported Job Attributes``

  - ExitCode
  - ExecutionHosts
  - Created
  - Started
  - Finished

``Supported Context Types``

  - *x509*: X509 proxy for gsissh
  - *userpass*: username/password pair for simple ssh
  - *ssh*: public/private keypair

``Supported Job Description Attributes``

  - Executable
  - Arguments
  - Environment
  - Input
  - Output
  - Error



Supported API Classes
=====================

  - :class:`saga.job.Service`
  - :class:`saga.job.Job`


saga.job.Service
""""""""""""""""

.. autoclass:: saga.adaptors.ssh.ssh_job.SSHJobService
   :members:


saga.job.Job
""""""""""""

.. autoclass:: saga.adaptors.ssh.ssh_job.SSHJob
   :members:



