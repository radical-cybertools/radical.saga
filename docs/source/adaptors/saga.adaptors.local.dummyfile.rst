
*****************************
saga.adaptors.local.dummyfile
*****************************

This adaptor interacts with local filesystem, by
using the (POSIX like) os and shutil Python packages.
It is named 'dummy', as this adaptor is only used
for testing and debugging -- it is *not* good for
production.


Version
=======

v0.1beta


Supported Schemas
=================

  - **dummy** : an invented schema.



Configuration Options
=====================

``enabled``

enable / disable saga.adaptors.local.dummyfile adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]
``enable_ctrl_c``

install SIGINT signal handler to abort application.

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]
``load_beta_adaptors``

load adaptors which are marked as beta (i.e. not released).

  - **type** : <type 'bool'>
  - **default** : False
  - **environment** : None
  - **valid options** : [True, False]


Supported Capabilities
======================



Supported API Classes
=====================

  - :class:`saga.filesystem.Directory`
  - :class:`saga.filesystem.File`


saga.filesystem.Directory
"""""""""""""""""""""""""

.. autoclass:: saga.adaptors.local.dummyfile.DummyDirectory
   :members:


saga.filesystem.File
""""""""""""""""""""

.. autoclass:: saga.adaptors.local.dummyfile.DummyFile
   :members:



