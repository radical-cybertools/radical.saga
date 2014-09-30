
#######################
saga.adaptor.shell_file
#######################

Description
-----------
 
The shell file adaptor. This adaptor uses the sh command line tools (sh,
ssh, gsissh) to access remote filesystems.




Supported Schemas
-----------------

This adaptor is triggered by the following URL schemes:

======================== ============================================================
schema                   description                                                 
======================== ============================================================
**ssh://**               use sftp to access remote filesystems                       
**sftp://**              use sftp to access remote filesystems                       
**file://**              use /bin/sh to access local filesystems                     
**local://**             alias for file://                                           
**gsissh://**            use gsisftp to access remote filesystems                    
**gsisftp://**           use gsisftp to access remote filesystems                    
======================== ============================================================



Example
-------

NO EXAMPLE AVAILABLE


Configuration Options
---------------------
Configuration options can be used to control the adaptor's     runtime behavior. Most adaptors don't need any configuration options     to be set in order to work. They are mostly for controlling experimental     features and properties of the adaptors.

     .. seealso:: More information about configuration options can be found in     the :ref:`conf_file` section.

enabled
*******

enable / disable saga.adaptor.shell_file adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]



Capabilities
------------

Supported Monitorable Metrics
*****************************


Supported Context Types
***********************

============================================================ ============================================================
                                                   Attribute Description
============================================================ ============================================================
                             :ref:`security_contexts` : x509 X509 proxy for gsissh
                         :ref:`security_contexts` : userpass username/password pair for ssh
                              :ref:`security_contexts` : ssh public/private keypair
============================================================ ============================================================



Supported API Classes
---------------------

This adaptor supports the following API classes:
  - :class:`saga.namespace.Directory`
  - :class:`saga.namespace.Entry`
  - :class:`saga.filesystem.Directory`
  - :class:`saga.filesystem.File`

Method implementation details are listed below.

saga.namespace.Directory
************************

.. autoclass:: saga.adaptors.shell.shell_file.ShellDirectory
   :members:


saga.namespace.Entry
********************

.. autoclass:: saga.adaptors.shell.shell_file.ShellFile
   :members:


saga.filesystem.Directory
*************************

.. autoclass:: saga.adaptors.shell.shell_file.ShellDirectory
   :members:


saga.filesystem.File
********************

.. autoclass:: saga.adaptors.shell.shell_file.ShellFile
   :members:



