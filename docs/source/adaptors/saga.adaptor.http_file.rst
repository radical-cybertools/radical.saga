
######################
saga.adaptor.http_file
######################

Description
-----------
The HTTP file adpator allows file transfer (copy) from remote resources to the local machine via the HTTP/HTTPS protocol, similar to cURL.


Supported Schemas
-----------------

This adaptor is triggered by the following URL schemes:

======================== ============================================================
schema                   description                                                 
======================== ============================================================
**http://**              use the http protocol to access a remote file               
**https://**             use the https protocol to access a remote file              
======================== ============================================================



Example
-------

.. literalinclude:: ../../../examples/files/http_file_copy.py


Configuration Options
---------------------
Configuration options can be used to control the adaptor's     runtime behavior. Most adaptors don't need any configuration options     to be set in order to work. They are mostly for controlling experimental     features and properties of the adaptors.

     .. seealso:: More information about configuration options can be found in     the :ref:`conf_file` section.

enabled
*******

enable / disable saga.adaptor.http_file adaptor

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
                         :ref:`security_contexts` : userpass username/password pair for ssh
============================================================ ============================================================



Supported API Classes
---------------------

This adaptor supports the following API classes:
  - :class:`saga.namespace.Entry`
  - :class:`saga.filesystem.File`

Method implementation details are listed below.

saga.namespace.Entry
********************

.. autoclass:: saga.adaptors.http.http_file.HTTPFile
   :members:


saga.filesystem.File
********************

.. autoclass:: saga.adaptors.http.http_file.HTTPFile
   :members:



