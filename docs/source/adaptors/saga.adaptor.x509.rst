
*****************
saga.adaptor.x509
*****************

This adaptor points to a X509 proxy, or certificate,
be used for backend connections.  Note that this
context can be created by a MyProxy context instance.

Version
=======

v0.1


Supported Schemas
=================

  - **x509** : x509 token information.



Configuration Options
=====================

``enabled``

enable / disable saga.adaptor.x509 adaptor

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

.. autoclass:: saga.adaptors.context.x509.ContextX509
   :members:



