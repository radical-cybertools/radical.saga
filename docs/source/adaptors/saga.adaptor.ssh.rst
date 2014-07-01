
################
saga.adaptor.ssh
################

Description
-----------
 
    
This SSH :class:`saga.Context` adaptor points to an ssh keypair and a user_id
to be used for ssh based backend connections.  For example, an ssh context can
be used to start jobs (:class:`saga.job.Job`) via ssh, to copy files
(:class:`saga.filesystem.File`) via sftp, etc.

Not all supported attributes have to be defined when using an ssh context
adaptor -- unspecified attributes will have sensible default values.  For
example, the ``c.user_id`` will default to the local user id, and the default
passphrase in ``c.user_pass`` will be empty.

The `UserKey` and `UserCert` attributes can point to either the public or
private key of the ssh keypair -- the SAGA-Python implementation will internally
complete the respective other key (public key file names are expected to be
derived from the private key, by appending the suffix `.pub` -- `.pem` files are
expected to contain both public and private key.).
    


Example
-------

.. literalinclude:: ../../../examples/context/context_ssh.py


Capabilities
------------

Supported Context Attributes
****************************

============================================================ ============================================================
                                                   Attribute Description
============================================================ ============================================================
                           :ref:`security_contexts` : UserID user name on target machine
                         :ref:`security_contexts` : UserPass passphrase for encrypted keys
                         :ref:`security_contexts` : UserCert maps to private ssh key
                             :ref:`security_contexts` : Type This MUST be set to ssh
                          :ref:`security_contexts` : UserKey maps to public ssh key
============================================================ ============================================================



