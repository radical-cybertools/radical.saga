
################
saga.adaptor.ssh
################

Description
-----------
This adaptor points to a ssh public/private keypair and 
user_id to be used for backend connections.



Example
-------

.. literalinclude:: ../../../examples/context/context_ssh.py


Capabilities
------------

Supported Context Attributes
****************************

========================= ============================================================
                Attribute Description
========================= ============================================================
                   UserID user name on target machine
                 UserPass passphrase for encryped keys
                 UserCert maps to the public ssh key
                     Type This MUST be set to ssh
                  UserKey maps to the public ssh key
========================= ============================================================



