
********************
saga.adaptor.myproxy
********************

This adaptor fetches an X509 proxy from
MyProxy when it is added to a saga.Session.

Version
=======

v0.1


Supported Schemas
=================

**myproxy** : this adaptor can only interact with myproxy backends



Configuration Options
=====================

enabled
-------

enable / disable saga.adaptor.myproxy adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]


Supported API Classes
=====================

  - :class:`saga.Context`


saga.Context
------------

.. autoclass:: saga.adaptors.context.myproxy.ContextMyProxy
   :members:



