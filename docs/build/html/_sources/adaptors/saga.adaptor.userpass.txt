
*********************
saga.adaptor.userpass
*********************

This adaptor stores user_id and user_pass tokens, to
be used for backend connections.

Version
=======

v0.1


Supported Schemas
=================

  - **userpass** : this adaptor can only store username/password pairs.



Configuration Options
=====================

enabled
-------

enable / disable saga.adaptor.userpass adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]


Supported API Classes
=====================

  - :class:`saga.Context`


saga.Context
------------

.. autoclass:: saga.adaptors.context.userpass.ContextUserPass
   :members:



