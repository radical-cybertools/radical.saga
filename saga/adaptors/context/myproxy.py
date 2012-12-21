
import os
import subprocess

from   saga.utils.singleton import Singleton

import saga.cpi.base
import saga.cpi.context

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async

######################################################################
#
# adaptor meta data
#
_adaptor_schema   = 'MyProxy'
_adaptor_name     = 'saga.adaptor.myproxy'
_adaptor_options  = []
_adaptor_info     = {
    'name'        : _adaptor_name,
    'cpis'        : [{ 
        'type'    : 'saga.Context',
        'class'   : 'ContextMyProxy',
        'schemas' : [_adaptor_schema]
        }
    ]
}


###############################################################################
# The adaptor class

class Adaptor (saga.cpi.base.AdaptorBase):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.

    We only need one instance of this adaptor per process (actually per engine,
    but engine is a singleton, too...) -- the engine will though create new CPI
    implementation instances as needed (one per SAGA API object).
    """

    __metaclass__ = Singleton


    def __init__ (self) :

        saga.cpi.base.AdaptorBase.__init__ (self, _adaptor_name, _adaptor_options)


    def register (self) :
        """ Adaptor registration function. The engine calls this during startup. 
    
            We usually do sanity checks here and throw and exception if we think
            the adaptor won't work in a given environment. In that case, the
            engine won't add it to it's internal list of adaptors. If everything
            is ok, we return the adaptor info.
        """
    
        return _adaptor_info


######################################################################
#
# job adaptor class
#
class ContextMyProxy (saga.cpi.Context) :

    def __init__ (self, api, adaptor) :
        saga.cpi.Base.__init__ (self, api, adaptor, 'ContextMyProxy')



    @SYNC
    def init_instance (self, type) :

        if type.lower () != _adaptor_schema.lower () :
            raise saga.exceptions.BadParameter \
                    ("the MyProxy context adaptor only handles MyProxy contexts - duh!")

        self._api.type = type


    @SYNC
    def _initialize (self, session) :

        # make sure we have server, username, password
        api = self._get_api ()


        # set up the myproxy command
        cmd = ""
        
        if api.user_pass :
            cmd = "echo %s | myproxy-logon --stdin_pass"  %  api.user_pass
        else :
            cmd = "myproxy-logon --stdin_pass"

        if api.server :
            (server, port) = api.server.split (':', 2)
            if server    : cmd += " --pshost %s"          %  server
            if port      : cmd += " --psport %s"          %  port
        if api.user_id   : cmd += " --username %s"        %  api.user_id
        if api.life_time : cmd += " --proxy_lifetime %s"  %  api.life_time

        # store the proxy in a private location
        proxy_store    = "%s/.saga/proxies/"   %  os.environ['HOME']
        proxy_location = "%s/myproxy_%d.x509"  %  (proxy_store, id(self))

        if not os.path.exists (proxy_store) :
            try :
                os.makedirs (proxy_store)
            except OSError as e :
                raise saga.exceptions.NoSuccess ("could not create myproxy store: %s"  %  str(e))

        cmd += " --out %s"  %  proxy_location

        expected_result  = "A credential has been received for user %s in %s.\n" \
                         %  (api.user_id, proxy_location)

        process          = subprocess.Popen (cmd, shell=True,
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
        (stdout, stderr) = process.communicate ()

        if expected_result == stdout :
            self._logger.info (stdout)
        else :
            self._logger.info (stderr)
            raise saga.exceptions.BadParameter ("could not evaluate myproxy context: %s"  %  stderr)

        new_ctx = saga.Context ('X509')

        new_ctx.user_proxy = proxy_location
        new_ctx.life_time  = api.lifetime

        session.add_context (new_ctx)


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

