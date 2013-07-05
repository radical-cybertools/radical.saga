
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import os
import sys
import pwd
import string
import getpass
import threading

import saga
import saga.exceptions         as se
import saga.utils.logger       as sul
import saga.utils.singleton    as sus
import saga.utils.pty_process  as supp

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
_SCHEMAS_GSI = ['gsissh', 'gsiscp', 'gsisftp', 'gsiftp']

_SCHEMAS = _SCHEMAS_SH + _SCHEMAS_SSH + _SCHEMAS_GSI

# FIXME: '-o ControlPersist' is only supported for newer ssh versions.  We
# should add detection, and enable that if available -- for now, just diable it.
#
# FIXME: we should use '%n' instead of '%h', but that is not supported by older
# ssh versions...

# ssh master/slave flag magic # FIXME: make timeouts configurable
_SSH_FLAGS_MASTER   = "-o ControlMaster=yes -o ControlPath=%(ctrl)s"
_SSH_FLAGS_SLAVE    = "-o ControlMaster=no  -o ControlPath=%(ctrl)s"

# FIXME: right now, we create a shell connection as master --
# but a master does not actually need a shell, as it is never really
# used to run commands...
_SCRIPTS = {
    'ssh' : { 
        'master'        : "%(ssh_env)s %(ssh_exe)s   %(ssh_args)s  %(m_flags)s  %(host_str)s",
        'shell'         : "%(ssh_env)s %(ssh_exe)s   %(ssh_args)s  %(s_flags)s  %(host_str)s",
      # 'copy_to'       : "%(scp_env)s %(scp_exe)s   %(scp_args)s  %(s_flags)s  %(src)s %(root)s/%(tgt)s",
      # 'copy_from'     : "%(scp_env)s %(scp_exe)s   %(scp_args)s  %(s_flags)s  %(root)s/%(src)s %(tgt)s",
        'copy_to'       : "%(sftp_env)s %(sftp_exe)s %(sftp_args)s %(s_flags)s  %(host_str)s",
        'copy_from'     : "%(sftp_env)s %(sftp_exe)s %(sftp_args)s %(s_flags)s  %(host_str)s",
        'copy_to_in'    : "progress \n put %(cp_flags)s %(src)s %(tgt)s \n exit \n",            
        'copy_from_in'  : "progress \n get %(cp_flags)s %(src)s %(tgt)s \n exit \n",
    },
    'sh' : { 
        'master'        : "%(sh_env)s %(sh_exe)s  %(sh_args)s",
        'shell'         : "%(sh_env)s %(sh_exe)s  %(sh_args)s",
        'copy_to'       : "%(sh_env)s %(sh_exe)s  %(sh_args)s",
        'copy_from'     : "%(sh_env)s %(sh_exe)s  %(sh_args)s",
        'copy_to_in'    : "cd ~ && exec %(cp_exe)s %(cp_flags)s %(src)s %(tgt)s",
        'copy_from_in'  : "cd ~ && exec %(cp_exe)s %(cp_flags)s %(src)s %(tgt)s",
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
    after ``ControlPersist`` seconds (see options).

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

    __metaclass__ = sus.Singleton


    # --------------------------------------------------------------------------
    #
    def __init__ (self) :

        self.logger   = sul.getLogger ('PTYShellFactory')
        self.registry = {}
        self.rlock    = threading.RLock ()

        self.logger.debug ("PTYShellFactory init %s" % self)

    # --------------------------------------------------------------------------
    #
    def initialize (self, url, session=None, logger=None) :

        with self.rlock :

            # make sure we have a valid session, and a valid url type
            if  not session :
                session = saga.Session (default=True)

            url = saga.Url (url)

            if  not logger :
                logger = sul.getLogger ('PTYShellFactory')

            # collect all information we have/need about the requested master
            # connection
            info = self._create_master_entry (url, session, logger)

            # we got master info - register the master, and create the instance!
            type_s = str(info['type'])
            user_s = str(info['user'])
            host_s = str(info['host_str'])

            # Now, if we don't have that master, yet, we need to instantiate it
            if not host_s in self.registry                 : self.registry[host_s] = {}
            if not user_s in self.registry[host_s]         : self.registry[host_s][user_s] = {}
            if not type_s in self.registry[host_s][user_s] :

                # new master: create an instance, and register it
                m_cmd = _SCRIPTS[info['type']]['master'] % info

                self.logger.debug ("open master pty for [%s] [%s] %s: %s'" \
                                % (type_s, host_s, user_s, m_cmd))

                info['pty'] = saga.utils.pty_process.PTYProcess (m_cmd, logger=logger)
                if not info['pty'].alive () :
                    raise se.NoSuccess._log (logger, \
                	  "Shell not connected to %s" % info['host_str'])

                # authorization, prompt setup, etc
                self._initialize_pty (info['pty'], info)

                # master was created - register it
                self.registry[host_s][user_s][type_s] = info


            else :
                # we already have a master: make sure it is alive, and restart as
                # needed
                info = self.registry[host_s][user_s][type_s]

                if  not info['pty'].alive (recover=True) :
                    raise se.IncorrectState._log (logger, \
                	  "Lost shell connection to %s" % info['host_str'])

            return info


    # --------------------------------------------------------------------------
    #
    def _initialize_pty (self, pty_shell, info) :

        with self.rlock :

            shell_pass = info['pass']
            key_pass   = info['key_pass']
            logger     = info['logger']
            latency    = info['latency']

            pty_shell.latency = latency

            # if we did not see a decent prompt within 'delay' time, something
            # went wrong.  Try to prompt a prompt (duh!)  Delay should be
            # minimum 0.1 second (to avoid flooding of local shells), and at
            # maximum 1 second (to keep startup time reasonable)
            # most one second.  We try to get within that range with 10*latency.
            delay = min (1.0, max (0.1, 50 * latency))

            if True : # FIXME
          # try :
                prompt_patterns = ["[Pp]assword:\s*$",                   # password   prompt
                                   "Enter passphrase for key '.*':\s*$", # passphrase prompt
                                   "want to continue connecting",        # hostkey confirmation
                                   ".*HELLO_\\d+_SAGA(.*)$",             # prompt detection helper
                                   "^(.*[\$#%>])\s*$"]                   # greedy native shell prompt 

                # find a prompt
                n, match = pty_shell.find (prompt_patterns, delay)

                # this loop will run until we finally find the shell prompt, or
                # if we think we have tried enough and give up.  On success
                # we'll try to set a different prompt, and when we found that,
                # too, we exit the loop and are be ready to running shell
                # commands.
                retries = 0
                while True :

                    # --------------------------------------------------------------
                    if n == None :

                        # we found none of the prompts, yet, and need to try
                        # again.  But to avoid hanging on invalid prompts, we
                        # print 'HELLO_SAGA', and search for that one, too

                        if retries > 50 :
                            raise se.NoSuccess ("Could not detect shell prompt (timeout)")

                        retries += 1
                        pty_shell.write ("printf 'HELLO_%%d_SAGA\\n' %d\n" % retries)

                        # FIXME:  consider timeout
                        n, match = pty_shell.find (prompt_patterns, delay)


                    # --------------------------------------------------------------
                    elif n == 0 :
                        logger.info ("got password prompt")
                        if  not shell_pass :
                            raise se.AuthenticationFailed ("prompted for unknown password (%s)" \
                                                          % match)

                        pty_shell.write ("%s\n" % shell_pass)
                        n, match = pty_shell.find (prompt_patterns, delay)


                    # --------------------------------------------------------------
                    elif n == 1 :
                        logger.info ("got passphrase prompt : %s" % match)

                        start = string.find (match, "'", 0)
                        end   = string.find (match, "'", start+1)

                        if start == -1 or end == -1 :
                            raise se.AuthenticationFailed ("could not extract key name (%s)" % match)

                        key = match[start+1:end]

                        if  not key in key_pass    :
                            raise se.AuthenticationFailed ("prompted for unknown key password (%s)" \
                                                          % key)

                        pty_shell.write ("%s\n" % key_pass[key])
                        n, match = pty_shell.find (prompt_patterns, delay)


                    # --------------------------------------------------------------
                    elif n == 2 :
                        logger.info ("got hostkey prompt")
                        pty_shell.write ("yes\n")
                        n, match = pty_shell.find (prompt_patterns, delay)


                    # --------------------------------------------------------------
                    elif n == 3 :
                        logger.info ("got shell prompt trigger (%s) (%s)" %  (n, match))

                        # one of the trigger commands got through -- we are
                        # happy to declare success, ignore any further output,
                        # and set a 'real' prompt.
                        break


                    # --------------------------------------------------------------
                    elif n == 4 :
                        logger.info ("got initial shell prompt (%s) (%s)" %  (n, match))

                        # we are done waiting for a prompt
                        break
                
                
          # except Exception as e :
          #     print e
          #     raise self._translate_exception (e)


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

        # authorization, prompt setup, etc
        self._initialize_pty (sh_slave, info)

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

        self._initialize_pty (cp_slave, info)

        cp_slave.write ("%s\n" % s_in)
        cp_slave.wait  ()

        if  cp_slave.exit_code != 0 :
            raise se.NoSuccess._log (info['logger'], "file copy failed: %s" % cp_slave.cache[-256:])

        info['logger'].debug ("copy done")


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

        self._initialize_pty (cp_slave, info)

        cp_slave.write ("%s\n" % s_in)
        cp_slave.wait  ()

        if  cp_slave.exit_code != 0 :
            raise se.NoSuccess._log (info['logger'], "file copy failed: %s" % cp_slave.cache[-256:])

        info['logger'].debug ("copy done")


    # --------------------------------------------------------------------------
    #
    def _create_master_entry (self, url, session, logger) :
        # FIXME: cache 'which' results, etc
        # FIXME: check 'which' results

        with self.rlock :

            info = {}

            info['schema']    = url.schema.lower ()
            info['host_str']  = url.host
            info['logger']    = logger
            info['pass']      = ""
            info['key_pass']  = {}

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
                info['sh_args']  = "-i"
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
                raise se.BadParameter._log (self.logger, \
                	  "cannot handle schema '%s://'" % url.schema)


            # depending on type, create command line (args, env etc)
            #
            # We always set term=vt100 to avoid ansi-escape sequences in the prompt
            # and elsewhere.  Also, we have to make sure that the shell is an
            # interactive login shell, so that it interprets the users startup
            # files, and reacts on commands.

            try :
                import saga.utils.misc as sumisc
                info['latency'] = sumisc.get_host_latency (url)

            except Exception  as e :
                info['latency'] = 1.0  # generic value assuming slow link
                info['logger'].warning ("Could not contact host '%s': %s" % (url, e))
                
            if  info['type'] == "sh" :

                if not sumisc.host_is_local (url.host) :
                    raise se.BadParameter._log (self.logger, \
                            "expect local host for '%s://', not '%s'" % (url.schema, url.host))

                if  'user' in info and info['user'] :
                    pass
                else :
                    info['user'] = getpass.getuser ()

            else :
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

                            if  context.attribute_exists ("user_id") and context.user_id :
                                info['user']  = context.user_id

                            if  context.attribute_exists ("user_key")  and  context.user_key  :
                                info['ssh_args']  += "-o IdentityFile=%s " % context.user_key 
                                info['scp_args']  += "-o IdentityFile=%s " % context.user_key 
                                info['sftp_args'] += "-o IdentityFile=%s " % context.user_key 

                                if  context.attribute_exists ("user_pass") and context.user_pass :
                                    info['key_pass'][context.user_key] = context.user_pass

                    if  context.type.lower () == "userpass" :
                        if  info['schema'] in _SCHEMAS_SSH + _SCHEMAS_GSI :
                            if  context.attribute_exists ("user_id") and context.user_id :
                                info['user']       = context.user_id
                            if  context.attribute_exists ("user_pass") and context.user_pass :
                                info['pass']       = context.user_pass

                    if  context.type.lower () == "x509" :
                        if  info['schema'] in _SCHEMAS_GSI :

                            if  context.attribute_exists ("user_proxy")  and   context.user_proxy :
                                info['ssh_env']   += "X509_USER_PROXY='%s' " % context.user_proxy
                                info['scp_env']   += "X509_USER_PROXY='%s' " % context.user_proxy
                                info['sftp_env']  += "X509_USER_PROXY='%s' " % context.user_proxy
                   
                            if  context.attribute_exists ("user_cert")   and  context.user_cert :
                                info['ssh_env']   += "X509_USER_CERT='%s' " % context.user_cert
                                info['scp_env']   += "X509_USER_CERT='%s' " % context.user_cert
                                info['sftp_env']  += "X509_USER_CERT='%s' " % context.user_cert
                   
                            if  context.attribute_exists ("user_key")    and  context.user_key :
                                info['ssh_env']   += "X509_USER_key='%s' "  % context.user_key
                                info['scp_env']   += "X509_USER_key='%s' "  % context.user_key
                                info['sftp_env']  += "X509_USER_key='%s' "  % context.user_key
                   
                            if  context.attribute_exists ("cert_repository") and context.cert_repository :
                                info['ssh_env']   += "X509_CERT_DIR='%s' "  % context.cert_repository
                                info['scp_env']   += "X509_CERT_DIR='%s' "  % context.cert_repository
                                info['sftp_env']  += "X509_CERT_DIR='%s' "  % context.cert_repository

                if url.port and url.port != -1 :
                    info['ssh_args']  += "-p %d " % int(url.port)
                    info['scp_args']  += "-p %d " % int(url.port)
                    info['sftp_args'] += "-P %d " % int(url.port)


                # all ssh based shells allow for user_id and user_pass from contexts
                # -- but the data given in the URL take precedence

                if url.username   :  info['user'] = url.username
                if url.password   :  info['pass'] = url.password

                ctrl_user = pwd.getpwuid (os.getuid ()).pw_name
                ctrl_base = "/tmp/saga_ssh_%s" % ctrl_user


                if  'user' in info and info['user'] :
                    info['host_str'] = "%s@%s"  % (info['user'], info['host_str'])
                    info['ctrl'] = "%s_%%h_%%p.%s.%s.ctrl" % (ctrl_base, os.getpid (), info['user'])
                else :
                    info['user'] = getpass.getuser ()
                    info['ctrl'] = "%s_%%h_%%p.%s.ctrl" % (ctrl_base, os.getpid ())

                info['m_flags']  = _SSH_FLAGS_MASTER % ({'ctrl' : info['ctrl']})
                info['s_flags']  = _SSH_FLAGS_SLAVE  % ({'ctrl' : info['ctrl']})
                info['fs_root']  = url

                info['fs_root'].path = "/"


            # keep all collected info in the master dict, and return it for
            # registration
            return info


    # ----------------------------------------------------------------
    #
    def _translate_exception (self, e) :
        """
        In many cases, we should be able to roughly infer the exception cause
        from the error message -- this is centrally done in this method.  If
        possible, it will return a new exception with a more concise error
        message and appropriate exception type.
        """

        if  not issubclass (e.__class__, se.SagaException) :
            # we do not touch non-saga exceptions
            return e

        if  not issubclass (e.__class__, se.NoSuccess) :
            # this seems to have a specific cause already, leave it alone
            return e

        cmsg = e._plain_message
        lmsg = cmsg.lower ()

        if 'auth' in lmsg :
            e = se.AuthorizationFailed (cmsg)

        elif 'pass' in lmsg :
            e = se.AuthenticationFailed (cmsg)

        elif 'ssh_exchange_identification' in lmsg :
            e = se.AuthenticationFailed ("too frequent login attempts, or sshd misconfiguration: %s" % cmsg)

        elif 'denied' in lmsg :
            e = se.PermissionDenied (cmsg)

        elif 'shared connection' in lmsg :
            e = se.NoSuccess ("Insufficient system resources: %s" % cmsg)

        elif 'pty allocation' in lmsg :
            e = se.NoSuccess ("Insufficient system resources: %s" % cmsg)

        elif 'Connection to master closed' in lmsg :
            e = se.NoSuccess ("Connection failed (insufficient system resources?): %s" % cmsg)

        print e.traceback
        return e


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

