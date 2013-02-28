
import os

import saga.utils.singleton
import saga.utils.pty_process

# ------------------------------------------------------------------------------
#
# ssh options:
#   -e none         : no escape character
#   -M              : master mode for connection sharing
#   -S control_path : slave mode for connection sharing
#   -t              : force pty allocation
#   -x              : disable x11 forwarding
#   
#   ServerAliveInterval
#   CheckHostIP no
#   ConnectTimeout
#   ControlMaster  yes | no | no ...
#   ControlPath    $BASE/ssh_control_%n_%p.$$.sock
#                  %r (remote id)? would need inspection
#   ControlPersist 100  : close master after 100 seconds idle
#   EscapeChar     none : transparent for binary data
#   TCPKeepAlive   yes  : detect connection failure
#
#   LoginGraceTime seconds : disconnect if no login after n seconds
#
# ------------------------------------------------------------------------------

# these arrays help to map requested client schemas to master schemas
_SCHEMAS_SH  = ['sh', 'fork', 'local', 'file']
_SCHEMAS_SSH = ['ssh', 'scp', 'sftp']
_SCHEMAS_GSI = ['gsissh', 'gsiscp', 'gsisftp']   # 'gsiftp'?

_SCHEMAS = _SCHEMAS_SH + _SCHEMAS_SSH + _SCHEMAS_GSI


# ------------------------------------------------------------------------------
#
class PTYShellFactory (object) :
    """
    This is the place where all master and all client shell connections get
    created.  But also, this factory maintains a registry of master connections,
    to quickly spawn slave connections for any customer w/o repeated
    authorization overhead.  Masters are unique per
    a host/user/port/context/shell_type hash.

    Any ssh master connection in this registry can idle, and may thus shut down
    after ``ControlPersist`` seconds (see options), or when the PTYProcess level
    GC collects it whichever comes first. But also, the GC will automatically
    restart the connection on any invocation of :func:`get()`.  Note that the
    created clients are also under timeout GC management.

    data model::


      self.registry
        |
        +-- "host[:port]"
        |   |
        |   +-- str(context)
        |   |   |
        |   |   +-- "shell_type" (ssh)
        |   |   |   |
        |   |   |   +-- pty_process (gc_timeout)
        |   |   |   +-- shell_initialize()
        |   |   |   +-- shell_finalize()
        |   |   |   +-- shell_alive()
        |   |   |
        |   |   +-- ...
        |   |
        |   +-- ...
        |
        +-- ...

    When Slave connections are requested, a suitable master connection is looked
    for and used.  'Suitable' means: ssh master for scp and sftp slaves; gsissh
    for gsiscp and gsisftp slaves; and sh master for file slaves

    """

    __metaclass__ = saga.utils.singleton.Singleton


    # --------------------------------------------------------------------------
    #
    def __init__ (self) :


        self.logger = saga.utils.logger.getLogger ('PTYShellFactory')
        self.registry = {}

    # --------------------------------------------------------------------------
    #
    def register (self, url, session=None, initialize=None, finalize=None, logger=None) :
        """ 
        This initiates a master connection.  If there is a suitable master
        connection in the registry, it is re-used, and no new master connection
        is created.  If needed, the existing master connection is revived.  
        """

        # make sure we have a valid session, and a valid url type
        if  not session :
            session = saga.Session (default=True)

        url = saga.Url (url)


        # collect all information we have/need about the requested master
        # connection
        master = self._create_master_entry (url, session)

        # we got master info - register the master, and create the instance!
        typ_s  = str(master['type'])
        ctx_s  = str(master['ctx'])
        host_s = str(master['host'])

        if not host_s in self.registry :
            self.registry[host_s] = {}

        if not ctx_s in self.registry[host_s] :
            self.registry[host_s][ctx_s] = {}

        if typ_s in self.registry[host_s][ctx_s] :
            # already have a master connection
            return False

        # new master: create an instance, and register it
        self.logger.info ("open master pty for '%s / %s / %s'" \
                       % (typ_s, host_s, ctx_s))

        master['pty'] = saga.utils.pty_process.PTYProcess (master['cmd'], logger=logger)

        master['pty'].set_initialize_hook (initialize)
        master['pty'].set_finalize_hook   (finalize)

        return master['pty']


    # --------------------------------------------------------------------------
    #
    def get (self, hostname, username, context) :

        connection_id = "%s@%s" % (username, hostname)

        if not context in self.registry :
            return None

        if not connection_id in self.registry[context] :
            return None

        # FIXME: check if the returned pty_shell is still viable / alive, and
        # revive if not.

        return self.registry[context][connection_id]


    # --------------------------------------------------------------------------
    #
    def _create_master_entry (self, url, session) :
        # FIXME: cache 'which' results, etc

        schema  = url.schema.lower ()

        typ  = ""
        exe  = ""
        user = ""
        pwd  = ""
        host = ""
        
        # context use to open shell connection, used for registry hashing
        ctx  = "None"  


        host_string = url.host
        if url.port and url.port != -1 :
          host_string += ":%s" % url.port


        # find out what type of shell we have to deal with
        if  schema in _SCHEMAS_SSH :
            typ =  "ssh"
            exe =  saga.utils.which.which ("ssh")

        if  schema in _SCHEMAS_GSI :
            typ =  "ssh"
            exe =  saga.utils.which.which ("gsissh")

        if  schema  in _SCHEMAS_SH :
            typ =  "sh"

            if  "SHELL" in os.environ :
                exe =  saga.utils.which.which (os.environ["SHELL"])
            else :
                exe =  saga.utils.which.which ("sh")

        else :
            raise saga.BadParameter._log (logger, \
            	  "PTYShell utility can only handle %s schema URLs, not %s" \
                  % (_SCHEMAS, schema))


        # make sure we have something to run
        if  not exe :
            raise saga.BadParameter._log (logger, \
            	  "cannot handle %s://, no shell executable found" % schema)


        # depending on type, create command line (args, env etc)
        #
        # We always set term=vt100 to avoid ansi-escape sequences in the prompt
        # and elsewhere.  Also, we have to make sure that the shell is an
        # interactive login shell, so that it interprets the users startup
        # files, and reacts on commands.
        if  typ == "ssh" :

            env  =  "/usr/bin/env TERM=vt100 "  # avoid ansi escapes
            args =  "-t "                       # force pty

            for context in contexts :

                # ssh can also handle UserPass contexts, and ssh type contexts.
                # gsissh can handle the same, but also X509 contexts.

                if  context.type.lower () == "ssh" :
                    if  schema in ['ssh', 'gsissh'] :
                        if  context.attribute_exists ("user_id")  or \
                            context.attribute_exists ("user_key") :
                            if  context.attribute_exists ("user_id") :
                                user  = context.user_id
                            if  context.attribute_exists ("user_key") :
                                args += "-i %s " % context.user_key
                            ctx = context

                if  context.type.lower () == "userpass" :
                    if  schema in ['ssh', 'gsissh'] :
                        if  context.attribute_exists ("user_id")   or \
                            context.attribute_exists ("user_pass") :
                            if  context.attribute_exists ("user_id") :
                                user = context.user_id
                            if  context.attribute_exists ("user_pass") :
                                pwd  = context.user_pass
                            ctx = context

                if  context.type.lower () == "gsissh" :
                    if  schema in ['gsissh'] :
                        # FIXME: also use cert_dir etc.
                        if  context.attribute_exists ("user_proxy") :
                            env = "X509_PROXY='%s' " % context.user_proxy
                            ctx = context

            # all ssh based shells allow for user_id and user_pass from contexts
            # -- but the data given in the URL take precedence

            if url.username  :  user = url.username
            if url.password  :  pwd  = url.password

            if user : args += "-l %s " % user

            # build the ssh command line
            cmd = "%s %s %s -M yes %s" % (env, exe, args, host_string)

        # a local shell
        elif typ == "sh" :
            # Make sure we have an interactive login shell w/o ansi escapes.
            # Note that we redirect the shell's stderr to stdout -- pty-process
            # does not expose stderr separately...
            args =  "-l -i"
            env  =  "/usr/bin/env TERM=vt100"
            cmd  =  "%s %s %s" % (env, exe, args)


        # keep all collected info in the master dict, and return it for
        # registration
        return { 'type' : typ,
                 'url'  : url,
                 'ctx'  : ctx,
                 'env'  : env,
                 'exe'  : exe,
                 'args' : args,  # *will not* contain the master flag
                 'cmd'  : cmd,   # *will*     contain the master flag
                 'host' : host_string }


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

