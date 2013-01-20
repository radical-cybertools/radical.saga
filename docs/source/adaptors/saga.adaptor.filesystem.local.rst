
*****************************
saga.adaptor.filesystem.local
*****************************

This adaptor interacts with local filesystem, by
using the (POSIX like) os and shutil Python packages.

Version
=======

v0.2


Supported Schemas
=================

**local** : alias for *file*
**file** : local filesystem.



Configuration Options
=====================

enabled
-------

enable / disable saga.adaptor.filesystem.local adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]


Supported API Classes
=====================

  - :class:`saga.filesystem.Directory`
  - :class:`saga.filesystem.File`


saga.filesystem.Directory
-------------------------

.. autoclass:: saga.adaptors.local.localfile.LocalDirectory
   :members:


saga.filesystem.File
--------------------

.. autoclass:: saga.adaptors.local.localfile.LocalFile
   :members:



