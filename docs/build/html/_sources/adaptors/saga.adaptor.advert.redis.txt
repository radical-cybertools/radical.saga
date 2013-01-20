
*************************
saga.adaptor.advert.redis
*************************

This adaptor interacts with a redis server to
implement the advert API semantics.

Version
=======

v0.2


Supported Schemas
=================

  - **redis** : redis nosql backend.



Configuration Options
=====================

enabled
-------

enable / disable saga.adaptor.advert.redis adaptor

  - **type** : <type 'bool'>
  - **default** : True
  - **environment** : None
  - **valid options** : [True, False]


Supported API Classes
=====================

  - :class:`saga.advert.Directory`
  - :class:`saga.advert.Entry`


saga.advert.Directory
---------------------

.. autoclass:: saga.adaptors.redis.redis_advert.RedisDirectory
   :members:


saga.advert.Entry
-----------------

.. autoclass:: saga.adaptors.redis.redis_advert.RedisEntry
   :members:



