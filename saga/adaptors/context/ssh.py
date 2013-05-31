
__author__    = "Andre Merzky, Ole Weidner"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import os

import saga.context
import saga.adaptors.cpi.base
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
                           saga.context.USER_KEY  : "maps to the private ssh key",
                           saga.context.USER_PASS : "passphrase for encryped keys"}
}

_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS, 
    'capabilities'     : _ADAPTOR_CAPABILITIES,
    'description'      : """ 
    
This SSH :class:`saga.Context` adaptor points to a private ssh key and user_id
to be used for ssh based backend connections.  For example, an ssh context can
be used to start jobs (:class:`saga.job.Job`) via ssh, to copy files
(:class:`saga.filesystem.File`) via sftp, etc.

Not all supported attributes have to be defined when using an ssh context
adaptor -- unspecified attributes will have sensible default values.  For
example, the ``c.user_id`` will default to the local user id, and the default
passphrase in ``c.user_pass`` will be empty.

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
class Adaptor (saga.adaptors.cpi.base.AdaptorBase):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :

        saga.adaptors.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

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
            candidate_keys = glob.glob ("%s/.ssh/*" % os.environ['HOME'])

            for key in candidate_keys :
                pub  = "%s.pub" % key
            
                if  not os.path.exists (pub)  or \
                    not os.path.isfile (pub)  or \
                    not os.path.exists (key)  or \
                    not os.path.isfile (key)     :
                    self._logger.info ("incomplete SSH  context for key at %s" %  key)
                    continue


                try :
                    fh_pub = open (pub)
                    fh_key = open (key)

                except Exception as e:
                    self._logger.info ("unreadable SSH  context for key at %s" %  key)
                    continue


                fh_pub.close ()
                fh_key.close ()

                import subprocess
                
                if  not subprocess.call (["sh", "-c", "grep ENCRYPTED %s > /dev/null" % key]) :
                    # needs passphrase.  Great, but won't work for
                    # default contexts
                    self._logger.info ("ignore  SSH  context for key at %s (needs pass)" %  key)
                    continue

                c = saga.Context ('ssh')
                c.user_key  = key

                self._default_contexts.append (c)

                self._logger.info ("default SSH  context for key at %s" %  key)

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
            raise saga.exceptions.BadParameter \
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
        _pass = None

        
        if          _api.attribute_exists (saga.context.USER_KEY ) :
            _key  = _api.get_attribute    (saga.context.USER_KEY )
        if          _api.attribute_exists (saga.context.USER_PASS) :
            _pass = _api.get_attribute    (saga.context.USER_PASS)

        # for backward compatibility, we interpret cert as key, overwriting the
        # previous key (which in former times was the public ssh key which we
        # don't care about)
        if          _api.attribute_exists (saga.context.USER_CERT) :
            _key  = _api.get_attribute    (saga.context.USER_CERT)
            _api.set_attribute (saga.context.USER_KEY, _key)
            self._logger.warning ("using context.user_cert as context.user_key (%s)" % _key)


        if  not _key :
            # nothing to do, really
            return


        if  not os.path.exists (_key ) or \
            not os.path.isfile (_key )    :

            raise saga.exceptions.BadParameter ("ssh key inaccessible: %s" % (_key))


        try :
            fh_key  = open (_key )

        except Exception as e:
            raise saga.exceptions.PermissionDenied ("ssh key '%s' not readable: %s"
                                                 % (_key, e))
        else :
            fh_key .close ()


        import subprocess
        if  not subprocess.call (["sh", "-c", "grep ENCRYPTED %s > /dev/null" % _key]) :
            if  not _pass :
                raise saga.exceptions.PermissionDenied ("ssh key '%s' is encrypted, need password" % (_key))


        if  subprocess.call (["sh", "-c", "ssh-keygen -y -f %s -P %s > /dev/null"
                          % (_key, _pass)]) :
            raise saga.exceptions.PermissionDenied ("ssh key '%s' is encrypted, incorrect password" % (_key))


        self._logger.info ("init SSH context for key  at %s" %  _key)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

