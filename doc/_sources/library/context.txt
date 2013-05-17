.. _security_contexts:

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

This context stores a user id and password, to be used for backend connections.
This context can be used for SSH connections if it is preferred over 
public-/private-key authentication.

The following context attributes are supported:

.. data::  Contex("UserPass")

    The type for this context has to be set to "UserPass" in the constructor, 
    i.e., ``saga.Context("ssh")``.

.. data::  saga.context.USER_ID

    The username on the target resource. 

.. data::  saga.context.USER_PASS

    The pass-phrase to use.

    .. warning:: NEVER put plain-text passwords into your source file. It is a huge security risk! Reading passwords from the command line, and environment variable or a configuration file instead would be a much better option. 


**Example**::

    ctx = saga.Context("UserPass")

    ctx.user_id   = "johndoe"
    ctx.user_pass = "XXXXXXX"  # BAD BAD BAD - don't do this in a real app. 

    session = saga.Session()
    session.add_context(ctx)

    js = saga.job.Service("ssh://machine_y.futuregrid.org",
                          session=session)

SSH Context
-----------

This SSH :context points to a ssh public/private key-pair and user id to 
be used for any ssh-based backend connections, e.g., ``ssh://``, ``pbs+ssh://`` and so on.

The following context attributes are supported:

.. data::  Contex("SSH")

    The type for this context has to be set to "SSH" in the constructor, 
    i.e., ``saga.Context("SSH")``.

.. data::  saga.context.USER_ID

    The username on the target resource. 

.. data::  saga.context.USER_KEY

    The public ssh key file to use for the connection. This attribute is useful
    if an SSH key-pair other than the default one (in $HOME/.ssh/) is required to establish a connection.

.. data::  saga.context.USER_PASS

    The pass-phrase to use to decrypt a password-protected key.

    .. warning:: NEVER put plain-text passwords into your source file. It is a huge security risk! Reading passwords from the command line, and environment variable or a configuration file instead would be a much better option. 


**Example**::

    ctx = saga.Context("SSH")

    ctx.user_id  = "johndoe"
    ctx.user_key = "/home/johndoe/.ssh/key_for_machine_x"

    session = saga.Session()
    session.add_context(ctx)

    js = saga.job.Service("ssh://machine_x.futuregrid.org",
                          session=session)



X.509 Context
-------------

The X.509 context points to an existing, local X509 proxy.

The following context attributes are supported:

.. data::  Contex("X509")

    The type for this context has to be set to "X509" in the constructor, 
    i.e., ``saga.Context("X509")``.

.. data::  saga.context.USER_PROXY

    The X509 user proxy file to use for the connection. This attribute is useful
    if a proxy file other than the default one (in /tmp/x509_u<uid>) is required to establish a connection.


**Example**::

    ctx = saga.Context("X509")

    ctx.user_proxy = "/tmp/x509_u123_for_machine_y"

    session = saga.Session()
    session.add_context(ctx)

    js = saga.job.Service("gsissh://machine_y.futuregrid.org",
                          session=session)


MyProxy Context
---------------

The MyProxy context fetches a delegated X.509 proxy from a (Globus) myproxy server.

The following context attributes are supported:

.. data::  Contex("MyProxy")

    The type for this context has to be set to "MyProxy" in the constructor, 
    i.e., ``saga.Context("MyProxy")``.

.. data::  saga.context.SERVER

    The hostname of the myproxy server. 
    This is equivalent to ``myproxy-logon --pshost``.

.. data::  saga.context.USER_ID

    The username for the delegated proxy. 
    This is equivalent to ``myproxy-logon --username``.

.. data::  saga.context.LIFE_TIME

    The lifetime of the delegated proxy.
    This is equivalent to ``myproxy-logon --proxy_lifetime`` (default is 12h).

.. data::  saga.context.USER_PASS

    The password for the delegated proxy.

    .. warning:: NEVER put plain-text passwords into your source file. It is a huge security risk! Reading passwords from the command line, and environment variable or a configuration file instead would be a much better option. 


**Example**::

    c = saga.Context("MyProxy")

    c.server    = "myproxy.teragrid.org"
    c.user_id   = "johndoe"
    c.user_pass = "XXXXXXX"  # BAD BAD BAD - don't do this in a real app. 

    session = saga.Session()
    session.add_context(ctx)

    js = saga.job.Service("pbs+gsissh://gsissh.kraken.nics.xsede.org",
                           session=session)






