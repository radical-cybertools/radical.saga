
import os
import sys

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

# FIXME: '-o ControlPersist' is only supported for newer ssh versions.  We
# should add detection, and enable that if available -- for now, just diable it.
#
# FIXME: we should use '%n' instead of '%h', but that is not supported by older
# ssh versions...

# ssh master/slave flag magic # FIXME: make timeouts configurable
_SSH_CONTROL_PATH   = "~/.saga/adaptors/shell/ssh_%%h_%%p.%s.ctrl" % (os.getpid ())
_SSH_FLAGS_MASTER   = "-o ControlMaster=yes -o ControlPath=%s" % _SSH_CONTROL_PATH
_SSH_FLAGS_SLAVE    = "-o ControlMaster=no  -o ControlPath=%s" % _SSH_CONTROL_PATH

# FIXME: right now, we create a shell connection as master --
# but a master does not actually need a shell, as it is never really
# used to run commands...
_SCRIPTS = {
    'ssh' : { 
        'master'        : "%(ssh_env)s %(ssh_exe)s   %(ssh_args)s  %(m_flags)s      %(host_str)s",
        'shell'         : "%(ssh_env)s %(ssh_exe)s   %(ssh_args)s  %(s_flags)s      %(host_str)s",
      # 'copy_to'       : "%(scp_env)s %(scp_exe)s   %(scp_args)s  %(s_flags)s      %(src)s %(root)s/%(tgt)s",
      # 'copy_from'     : "%(scp_env)s %(scp_exe)s   %(scp_args)s  %(s_flags)s      %(root)s/%(src)s %(tgt)s",
        'copy_to'       : "%(sftp_env)s %(sftp_exe)s %(sftp_args)s %(s_flags)s -b - %(host_str)s",
        'copy_from'     : "%(sftp_env)s %(sftp_exe)s %(sftp_args)s %(s_flags)s -b - %(host_str)s",
        'copy_to_in'    : "progress \n put %(cp_flags) %(src)s %(tgt)s \n exit \n",            
        'copy_from_in'  : "progress \n get %(cp_flags) %(src)s %(tgt)s \n exit \n",
    },
    'sh' : { 
        'master'        : "%(sh_env)s %(sh_exe)s  %(sh_args)s",
        'shell'         : "%(sh_env)s %(sh_exe)s  %(sh_args)s",
        'copy_to'       : "%(cp_env)s %(sh_exe)s -c \"cd ~; %(cp_exe)s %(cp_flags)s %(src)s %(tgt)s\"",
        'copy_from'     : "%(cp_env)s %(sh_exe)s -c \"cd ~; %(cp_exe)s %(cp_flags)s %(src)s %(tgt)s\"",
        'copy_to_in'    : "",
        'copy_from_in'  : "",
    }
}

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
    def initialize (self, url, session=None, logger=None) :

        # make sure we have a valid session, and a valid url type
        if  not session :
            session = saga.Session (default=True)

        url = saga.Url (url)

        if  not logger :
            logger = saga.utils.logger.getLogger ('PTYShellFactory')

        # collect all information we have/need about the requested master
        # connection
        info = self._create_master_entry (url, session, logger)

        # we got master info - register the master, and create the instance!
        typ_s  = str(info['type'])
        ctx_s  = str(info['ctx'])
        host_s = str(info['host_str'])

        # Now, if we don't have that master, yet, we need to instantiate it
        if not host_s in self.registry                : self.registry[host_s] = {} 
        if not ctx_s  in self.registry[host_s]        : self.registry[host_s][ctx_s] = {}
        if not typ_s  in self.registry[host_s][ctx_s] : 

            # new master: create an instance, and register it
            self.logger.info ("open master pty for [%s] [%s] [%s]'" \
                           % (typ_s, host_s, ctx_s))

            m_cmd = _SCRIPTS[info['type']]['master'] % info
            info['pty'] = saga.utils.pty_process.PTYProcess (m_cmd, logger=logger)
            if not info['pty'].alive () :
                raise saga.NoSuccess._log (logger, \
            	  "Shell not connected to %s" % info['host_str'])

            # master was created - register it
            self.registry[host_s][ctx_s][typ_s] = info 


        else :
            # we already have a master: make sure it is alive, and restart as
            # needed
            info = self.registry[host_s][ctx_s][typ_s]

            if  not info['pty'].alive (recover=True) :
                raise saga.IncorrectState._log (logger, \
            	  "Lost shell connection to %s" % info['host_str'])

        return info


    # --------------------------------------------------------------------------
    #
    def run_shell (self, info) :
        """ 
        This initiates a master connection.  If there is a suitable master
        connection in the registry, it is re-used, and no new master connection
        is created.  If needed, the existing master connection is revived.  
        """

        s_cmd = _SCRIPTS[info['type']]['shell'] % info

        # at this point, we do have a valid, living master
        sh_slave = saga.utils.pty_process.PTYProcess (s_cmd, info['logger'])
        return sh_slave


    # --------------------------------------------------------------------------
    #
    def run_copy_to (self, info, src, tgt, cp_flags="") :
        """ 
        This initiates a slave copy connection.   Src is interpreted as local
        path, tgt as path on the remote host.
        """

        repl = dict ({'src'      : src, 
                      'tgt'      : tgt, 
                      'cp_flags' : cp_flags}.items ()+ info.items ())

        # at this point, we do have a valid, living master
        s_cmd = _SCRIPTS[info['type']]['copy_to']    % repl
        s_in  = _SCRIPTS[info['type']]['copy_to_in'] % repl

        cp_slave = saga.utils.pty_process.PTYProcess (s_cmd, info['logger'])
        cp_slave.write (s_in)

      # try :
      #     while True :
      #         print cp_slave.read ()
      # except :
      #     pass

        cp_slave.wait ()

        return cp_slave


    # --------------------------------------------------------------------------
    #
    def run_copy_from (self, info, src, tgt, cp_flags="") :
        """ 
        This initiates a slave copy connection.   Src is interpreted as path on
        the remote host, tgt as local path.
        """

        repl = dict ({'src'      : src, 
                      'tgt'      : tgt, 
                      'cp_flags' : cp_flags}.items ()+ info.items ())


        # at this point, we do have a valid, living master
        s_cmd = _SCRIPTS[info['type']]['copy_from']    % repl
        s_in  = _SCRIPTS[info['type']]['copy_from_in'] % repl
        cp_slave = saga.utils.pty_process.PTYProcess (s_cmd, info['logger'])
        cp_slave.write (s_in)

      # try :
      #     while True :
      #         print cp_slave.read ()
      # except :
      #     pass

        cp_slave.wait ()

        return cp_slave


    # --------------------------------------------------------------------------
    #
    def _create_master_entry (self, url, session, logger) :
        # FIXME: cache 'which' results, etc
        # FIXME: check 'which' results

        info = {}

        info['schema']   = url.schema.lower ()
        info['host_str'] = url.host
        info['logger']   = logger
        info['ctx']      = []

        if url.port and url.port != -1 :
          info['host_str'] += ":%s" % url.port


        # find out what type of shell we have to deal with
        if  info['schema']   in _SCHEMAS_SSH :
            info['type']     = "ssh"
            info['ssh_exe']  = saga.utils.which.which ("ssh")
            info['scp_exe']  = saga.utils.which.which ("scp")
            info['sftp_exe'] = saga.utils.which.which ("sftp")

        elif info['schema']  in _SCHEMAS_GSI :
            info['type']     = "ssh"
            info['ssh_exe']  = saga.utils.which.which ("gsissh")
            info['scp_exe']  = saga.utils.which.which ("gsiscp")
            info['sftp_exe'] = saga.utils.which.which ("gsisftp")

        elif info['schema']  in _SCHEMAS_SH :
            info['type']     = "sh"
            info['sh_args']  = "-l -i"
            info['sh_env']   = "/usr/bin/env TERM=vt100"
            info['cp_env']   = "/usr/bin/env TERM=vt100"
            info['fs_root']  = "/"

            if  "SHELL" in os.environ :
                info['sh_exe'] =  saga.utils.which.which (os.environ["SHELL"])
                info['cp_exe'] =  saga.utils.which.which ("cp")
            else :
                info['sh_exe'] =  saga.utils.which.which ("sh")
                info['cp_exe'] =  saga.utils.which.which ("cp")

        else :
            raise saga.BadParameter._log (self.logger, \
            	  "cannot handle schema '%s://'" % url.schema)


        # depending on type, create command line (args, env etc)
        #
        # We always set term=vt100 to avoid ansi-escape sequences in the prompt
        # and elsewhere.  Also, we have to make sure that the shell is an
        # interactive login shell, so that it interprets the users startup
        # files, and reacts on commands.

        if  info['type'] == "sh" :
            import saga.utils.misc as sum
            if not sum.host_is_local (url.host) :
                raise saga.BadParameter._log (self.logger, \
                        "expect local host for '%s://', not '%s'" % (url.schema, url.host))

        else :
            import saga.utils.misc as sum
            if not sum.host_is_valid (url.host) :
                raise saga.BadParameter._log (self.logger, "invalid host in '%s'" % (url))

            info['ssh_env']   =  "/usr/bin/env TERM=vt100 "  # avoid ansi escapes
            info['scp_env']   =  "/usr/bin/env TERM=vt100 "  # avoid ansi escapes
            info['sftp_env']  =  "/usr/bin/env TERM=vt100 "  # avoid ansi escapes
            info['ssh_args']  =  "-t "                       # force pty
            info['scp_args']  =  ""
            info['sftp_args'] =  ""

            for context in session.contexts :

                # ssh can also handle UserPass contexts, and ssh type contexts.
                # gsissh can handle the same, but also X509 contexts.

                if  context.type.lower () == "ssh" :
                    if  info['schema'] in _SCHEMAS_SSH + _SCHEMAS_GSI :
                        if  context.attribute_exists ("user_id")  or \
                            context.attribute_exists ("user_key") :
                            if  context.attribute_exists ("user_id") :
                                info['user']  = context.user_id
                            if  context.attribute_exists ("user_key") :
                                info['ssh_args']  += "-i %s " % context.user_key
                                info['scp_args']  += "-i %s " % context.user_key
                                info['sftp_args'] += "-i %s " % context.user_key
                            info['ctx'].append (context)

                if  context.type.lower () == "userpass" :
                    if  info['schema'] in _SCHEMAS_SSH + _SCHEMAS_GSI :
                        if  context.attribute_exists ("user_id")   or \
                            context.attribute_exists ("user_pass") :
                            if  context.attribute_exists ("user_id") :
                                info['user'] = context.user_id
                            if  context.attribute_exists ("user_pass") :
                                info['pwd']  = context.user_pass
                            info['ctx'].append (context)

                if  context.type.lower () == "x509" :
                    if  info['schema'] in _SCHEMAS_GSI :
                        # FIXME: also use cert_dir etc.
                        if  context.attribute_exists ("user_proxy") :
                            info['ssh_env']  += "X509_PROXY='%s' " % context.user_proxy
                            info['scp_env']  += "X509_PROXY='%s' " % context.user_proxy
                            info['sftp_env'] += "X509_PROXY='%s' " % context.user_proxy
                            info['ctx'].append (context)

            # all ssh based shells allow for user_id and user_pass from contexts
            # -- but the data given in the URL take precedence

            if url.username   :  info['user'] = url.username
            if url.password   :  info['pwd']  = url.password

            if 'user' in info : 
                info['host_str'] = "%s@%s"  % (info['user'], info['host_str'])

            info['m_flags']  = _SSH_FLAGS_MASTER
            info['s_flags']  = _SSH_FLAGS_SLAVE
            info['fs_root']  = url

            info['fs_root'].path = "/"


        # keep all collected info in the master dict, and return it for
        # registration
        return info

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

