
import os
import subprocess

import saga.cpi.base
import saga.cpi.context

from   saga.engine.logger import getLogger

SYNC  = saga.cpi.base.sync
ASYNC = saga.cpi.base.async

######################################################################
#
# adaptor meta data
#
_adaptor_type     =    'MyProxy'
_adaptor_name     =    'saga_adaptor_context_myproxy'
_adaptor_registry = [{ 'name'    : _adaptor_name,
                       'type'    : 'saga.Context',
                       'class'   : 'ContextMyProxy',
                       'schemas' : ['MyProxy']
                     }]


######################################################################
#
# adaptor registration
#
def register () :

    # perform some sanity checks, like check if dependencies are met
    return _adaptor_registry


######################################################################
#
# job adaptor class
#
class ContextMyProxy (saga.cpi.Context) :

    def __init__ (self, api) :
        saga.cpi.Base.__init__ (self, api, _adaptor_name)

        self._logger = getLogger (_adaptor_name)


    @SYNC
    def init_instance (self, type) :

        if type.lower () != _adaptor_type.lower () :
            raise saga.exceptions.BadParameter \
                    ("the myproxy context adaptor only handles myproxy contexts - duh!")

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


        print "type:   %s"  %  api.type
        print "user:   %s"  %  api.user_id
        print "pass:   %s"  %  api.user_pass
        print "server: %s"  %  api.server
        print "ttl:    %s"  %  api.life_time
        print "cmd:    %s"  %  cmd

        expected_result  = "A credential has been received for user %s in %s.\n" \
                         %  (api.user_id, proxy_location)

        process          = subprocess.Popen (cmd, shell=True,
                                             stdout=subprocess.PIPE,
                                             stderr=subprocess.PIPE)
        (stdout, stderr) = process.communicate ()

        print "stderr: '%s'"  %  str(stderr)
        print "stdout: '%s'"  %  str(stdout)
        print "expect: '%s'"  %  str(expected_result)

        if expected_result == stdout :
            self._logger.info (stdout)
        else :
            self._logger.info (stderr)

        new_ctx = saga.Context ('X509')
        new_ctx.user_proxy = proxy_location
        new_ctx.life_time = 1000

        session.add_context (new_ctx)

    


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

