
****************
saga.adaptor.ssh
****************

This adaptor points to a ssh public/private keypair and 
user_id to be used for backend connections.

Version
=======

v0.1


Supported Schemas
=================

  - **ssh** : ssh private/public and userid information.



Configuration Options
=====================

``enabled``

enable / disable saga.adaptor.ssh adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]


Supported Capabilities
======================

``attributes``

  - Type
  - UserProxy
  - LifeTime



Supported API Classes
=====================

  - :class:`saga.Context`


saga.Context
""""""""""""

.. autoclass:: saga.adaptors.context.ssh.ContextSSH
   :members:



