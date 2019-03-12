.. _security_contexts:

Security Contexts 
*****************

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

.. data::  saga.context.user_id

    The username on the target resource. 

.. data::  saga.context.user_pass

    The pass-phrase to use.

    .. warning:: NEVER put plain-text passwords into your source file. It is a huge security risk! Reading passwords from the command line, and environment variable or a configuration file instead would be a much better option. 


**Example**::

    ctx = saga.Context("UserPass")

    ctx.user_id   = "johndoe"
    ctx.user_pass = os.environ['MY_USER_PASS']

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

.. data::  saga.context.user_id

    The username on the target resource. 

.. data::  saga.context.user_key

    The public ssh key file to use for the connection. This attribute is useful
    if an SSH key-pair other than the default one (in $HOME/.ssh/) is required to establish a connection.

.. data::  saga.context.user_pass

    The pass-phrase to use to decrypt a password-protected key.

    .. warning:: NEVER put plain-text passwords into your source file. It is a huge security risk! Reading passwords from the command line, and environment variable or a configuration file instead would be a much better option. 


**Example**::

    ctx = saga.Context("SSH")

    ctx.user_id   = "johndoe"
    ctx.user_key  = "/home/johndoe/.ssh/key_for_machine_x"
    ctx.user_pass = "XXXX"  # password to decrypt 'user_key' (if required)

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

.. data::  saga.context.user_proxy

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

.. data::  saga.context.server

    The hostname of the myproxy server. 
    This is equivalent to ``myproxy-logon --pshost``.

.. data::  saga.context.user_id

    The username for the delegated proxy. 
    This is equivalent to ``myproxy-logon --username``.

.. data::  saga.context.life_time

    The lifetime of the delegated proxy.
    This is equivalent to ``myproxy-logon --proxy_lifetime`` (default is 12h).

.. data::  saga.context.user_pass

    The password for the delegated proxy.

    .. warning:: NEVER put plain-text passwords into your source file. It is a huge security risk! Reading passwords from the command line, and environment variable or a configuration file instead would be a much better option. 


**Example**::

    c = saga.Context("MyProxy")

    c.server    = "myproxy.teragrid.org"
    c.user_id   = "johndoe"
    c.user_pass = os.environ['MY_USER_PASS']

    session = saga.Session()
    session.add_context(ctx)

    js = saga.job.Service("pbs+gsissh://gsissh.kraken.nics.xsede.org",
                           session=session)


EC2 Context
-----------

The EC2 context can be used to authenticate against the Amazon EC2 service.

.. note:: EC2 Contexts are usually used in conjunction with an ``EC2_KEYPAIR``
   and an ``SSH Context`` as shown in the example below.

The following context attributes are supported:

.. data::  Contex("MyProxy")

    The type for this context has to be set to "EC2" in the constructor, 
    i.e., ``saga.Context("EC2")``.


.. data::  saga.context.user_id

    The Amazon EC2 ID. See the Amazon Web-Services website for more details.

.. data::  saga.context.user_key

    The Amazon EC2 key. See the Amazon Web-Services website for more details.


**Example**::

    ec2_ctx = saga.Context('EC2')
    ec2_ctx.user_id = 'XXXXXXXXXXYYYYYYYYZ'
    ec2_ctx.user_key = 'WwwwwwXxxxxxxxxxYyyyyyyyyZzzzzzz'

    # The SSH key-pair we want to use the access the EC2 VM. If the keypair is
    # not yet registered on EC2 saga will register it automatically.
    ec2keypair_ctx = saga.Context('EC2_KEYPAIR')
    ec2keypair_ctx.token = 'KeyName'
    ec2keypair_ctx.user_key = '$HOME/.ssh/ec2_key'
    ec2keypair_ctx.user_id = 'root'  # the user id on the target VM

    # The same SSH key-pair as above, but this one will be picked up by the SSH
    # adaptor. While this is somewhat redundant, it is still necessary because
    # of current limitations imposed by 'liblcoud', the library which implements
    # the radical.saga EC2 adaptor. 
    ssh_ctx = saga.Context('SSH')
    ssh_ctx.user_id = 'root'
    ssh_ctx.user_key = '$HOME/.ssh/ec2_key'

    session = saga.Session(False)  # FALSE: don't use other (default) contexts
    session.contexts.append(ec2_ctx)
    session.contexts.append(ec2keypair_ctx)
    session.contexts.append(ssh_ctx)


EC2_KEYPAIR Context
-------------------

This context refers to an SSH key-pair and is very similar to the ``SSH Context``
described above. It is used to inject a key-pair into an Amazon EC2 VM and 
is used injunction with an ``EC2 Context``. See above for an example.

The following context attributes are supported:

.. data::  Contex("EC2_KEYPAIR")

    The type for this context has to be set to "EC2_KEYPAIR" in the constructor, 
    i.e., ``saga.Context("EC2_KEYPAIR")``.

.. data::  saga.context.user_id

    The username on the target resource. 

.. data::  saga.context.user_key

    The public ssh key file to use for the connection. This attribute is useful
    if an SSH key-pair other than the default one (in $HOME/.ssh/) is required to establish a connection.

.. data::  saga.context.user_pass

    The pass-phrase to use to decrypt a password-protected key.

.. data:: saga.context.token

    The Amazon EC2 identifier for this key-pair. 


