
Security Contexts 
*****************

.. todo:: Intro to SAGA Contexts handling.

Context Class -- :mod:`saga.context`
------------------------------------

.. automodule:: saga.context
   :show-inheritance:
   :members: Context


UserPass Context
----------------


SSH Context
-----------


MyProxy Context
---------------

The MyProxy context fetches a delegated X.509 proxy via (Globus) myproxy.

The following context attributes are supported:

.. data::  saga.context.TYPE

    The type for this context has to be set to MyProxy.

.. data::  saga.context.SERVER

    The hostname of the myproxy server. 
    This is equivalent to ``myproxy-logon --pshost``.

.. data::  saga.context.USER_ID

    The username for the delegated proxy. 
    This is equivalent to ``myproxy-logon --username``.

.. data::  saga.context.USER_PASS

    The password for the delegated proxy.

.. data::  saga.context.LIFE_TIME

    The lifetime of the delegated proxy.
    This is equivalent to ``myproxy-logon --proxy_lifetime``.


Example::

    c = saga.Context

    c.type      = "MyProxy"
    c.server    = "myproxy.teragrid.org"
    c.user_id   = "johndoe"
    c.user_pass = "XXXXXXX"

    session = saga.Session()
    session.add_context(ctx)

    js = saga.job.Service("pbs+gsissh://gsissh.kraken.nics.xsede.org",
                           session=session)





X.509 Context
-------------

