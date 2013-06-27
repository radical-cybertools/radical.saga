
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import os

import saga.context
import saga.exceptions as se
import saga.adaptors.base
import saga.adaptors.cpi.context

SYNC_CALL  = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL

######################################################################
#
# adaptor meta data
#
_ADAPTOR_NAME          = 'saga.adaptor.ssh'
_ADAPTOR_SCHEMAS       = ['ssh']
_ADAPTOR_OPTIONS       = []

# FIXME: complete attribute list
_ADAPTOR_CAPABILITIES  = {
     'ctx_attributes'   : {saga.context.TYPE      : "This MUST be set to ssh",
                           saga.context.USER_ID   : "user name on target machine",
                           saga.context.USER_KEY  : "maps to public *or* private ssh key",
                           saga.context.USER_PASS : "passphrase for encryped keys"}
}

_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilities'     : _ADAPTOR_CAPABILITIES,
    'description'      : """ 
    
This SSH :class:`saga.Context` adaptor points to an ssh keypair and a user_id
to be used for ssh based backend connections.  For example, an ssh context can
be used to start jobs (:class:`saga.job.Job`) via ssh, to copy files
(:class:`saga.filesystem.File`) via sftp, etc.

Not all supported attributes have to be defined when using an ssh context
adaptor -- unspecified attributes will have sensible default values.  For
example, the ``c.user_id`` will default to the local user id, and the default
passphrase in ``c.user_pass`` will be empty.

The `UserKey` attribute can point to either the public or private key of the ssh
keypair -- the SAGA-Python implementation will internally complete the
repsective other key (public key file names are expected to be derived from the
private key, by appending the suffix `.pub`).
    """,

    'schemas'          : {'ssh' : 'ssh key and userid information.'},
    'example'          : "examples/context/context_ssh.py"
}

_ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.1',
    'schemas'          : _ADAPTOR_SCHEMAS,
    'cpis'             : [{ 
        'type'         : 'saga.Context',
        'class'        : 'ContextSSH'
        }
    ]
}


# ------------------------------------------------------------------------------
# 
class Adaptor (saga.adaptors.base.Base):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :

        saga.adaptors.base.Base.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        # there are no default myproxy contexts
        self._default_contexts = []
        self._have_defaults    = False


    # --------------------------------------------------------------------------
    #
    def sanity_check (self) :
        pass


    # --------------------------------------------------------------------------
    #
    def _get_default_contexts (self) :

        if not self._have_defaults :
 
            import glob
            candidate_certs = glob.glob ("%s/.ssh/*" % os.environ['HOME'])

            for cert in candidate_certs :

                if  cert.endswith ('.pub') :
                    # don't want public keys in this loop
                    continue

                if  cert.endswith ('.pem') :
                    key  = cert
                else :
                    key  = "%s.pub" % cert
            
                if  not os.path.exists (key)   or \
                    not os.path.isfile (key)   or \
                    not os.path.exists (cert)  or \
                    not os.path.isfile (cert)     :
                  # self._logger.info ("incomplete ssh key at %s" %  cert)
                    continue


                try :
                    fh_pub = open (key)
                    fh_key = open (cert)

                    fh_pub.close ()
                    fh_key.close ()

                except Exception as e:
                    self._logger.info ("invalid ssh key at %s (%s)" %  (cert, e))
                    continue


                import subprocess
                if  not subprocess.call (["sh", "-c", "grep ENCRYPTED %s > /dev/null" % cert]) :
                    # needs passphrase.  Great, actually, but won't work for
                    # default contexts as long as we can't prompt for pass
                    # phrases...
                    self._logger.warn ("ignore  ssh key at %s (requires passphrase)" %  cert)
                    continue

                c = saga.Context ('ssh')
                c.user_key  = key
                c.user_cert = cert

                self._default_contexts.append (c)

                self._logger.info ("default ssh key at %s" %  cert)

            self._have_defaults = True


        # have defaults, and can return them...
        return self._default_contexts



# ------------------------------------------------------------------------------
#
class ContextSSH (saga.adaptors.cpi.context.Context) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        _cpi_base = super  (ContextSSH, self)
        _cpi_base.__init__ (api, adaptor)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, type) :

        if not type.lower () in (schema.lower() for schema in _ADAPTOR_SCHEMAS) :
            raise se.BadParameter \
                    ("the ssh context adaptor only handles ssh contexts - duh!")

        self.get_api ().type = type

        return self.get_api ()


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def _initialize (self, session) :

        # make sure we have can access the key
        _api  = self.get_api ()

        _key  = None
        _cert = None
        _pass = None

        
        if          _api.attribute_exists (saga.context.USER_KEY ) :
            _key  = _api.get_attribute    (saga.context.USER_KEY )
        if          _api.attribute_exists (saga.context.USER_CERT) :
            _cert = _api.get_attribute    (saga.context.USER_CERT)
        if          _api.attribute_exists (saga.context.USER_PASS) :
            _pass = _api.get_attribute    (saga.context.USER_PASS)


        # either user_key or user_cert should be specified (or both), 
        # then we complement the other, and convert to/from private 
        # from/to public keys
        if  _cert and not _key :
            _key  = _cert

        if  _key and not _cert :
            _cert  = _key

        if  not _key :
            # nothing to do, really.  This likely means that ssh key setup is
            # done out-of-band.
            return

        # if we have an pem key, we don't do anything else
        if  _key.endswith ('.pem') :
            pass

        # otherwise we assume normal public/private key pairs
        else :

            # convert public key into private key
            if  _cert.endswith ('.pub') :
                _cert = _cert[:-4]

            # convert private key into public key
            if  not _key.endswith ('.pub') :
                _key += ".pub"

            # update the context with these setting
            _api.set_attribute (saga.context.USER_KEY , _key )
            _api.set_attribute (saga.context.USER_CERT, _cert)



        # the private and public keys must exist
        if  not os.path.exists (_key ) or \
            not os.path.isfile (_key )    :
            raise se.BadParameter ("ssh public  key inaccessible: %s" % (_key))

        if  not os.path.exists (_cert) or \
            not os.path.isfile (_cert)    :
            raise se.BadParameter ("ssh private key inaccessible: %s" % (_cert))


        try :
            fh_key  = open (_key)

        except Exception as e:
            raise se.PermissionDenied ("ssh key '%s' not readable: %s" % (_key, e))
        else :
            fh_key .close ()


        try :
            fh_cert = open (_cert)

        except Exception as e:
            raise se.PermissionDenied ("ssh key '%s' not readable: %s" % (_cert, e))
        else :
            fh_cert.close ()


        import subprocess
        if  not subprocess.call (["sh", "-c", "grep ENCRYPTED %s > /dev/null" % _cert]) :
            if  not _pass :
                raise se.PermissionDenied ("ssh key '%s' is encrypted, need password" % (_cert))


        if  subprocess.call (["sh", "-c", "ssh-keygen -y -f %s -P %s > /dev/null"
                          % (_cert, _pass)]) :
            raise se.PermissionDenied ("ssh key '%s' is encrypted, incorrect password" % (_cert))


        self._logger.info ("init SSH context for key  at '%s' done" %  _cert)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

