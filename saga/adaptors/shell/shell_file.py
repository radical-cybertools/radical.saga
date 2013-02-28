
""" shell based file adaptor implementation """

import saga.utils.which
import saga.utils.pty_shell

import saga.adaptors.cpi.base
import saga.adaptors.cpi.filesystem

from   saga.filesystem.constants import *

import re
import os
import time
import threading

import shell_wrapper

SYNC_CALL  = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL


# --------------------------------------------------------------------
# some private defs
#
_PTY_TIMEOUT = 2.0

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "saga.adaptor.shell_file"
_ADAPTOR_SCHEMAS       = ["file", "local", "ssh", "gsissh"]
_ADAPTOR_OPTIONS       = [
    { 
    'category'         : 'saga.adaptor.shell_file',
    'name'             : 'enable_notifications', 
    'type'             : bool, 
    'default'          : False,
    'valid_options'    : [True, False],
    'documentation'    : '''Enable support for filesystem notifications.  Note that
                          enabling this option will create a local thread and a remote 
                          shell process.''',
    'env_variable'     : None
    }
]

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES  = {
    "metrics"          : [],
    "contexts"         : {"ssh"      : "public/private keypair",
                          "x509"     : "X509 proxy for gsissh",
                          "userpass" : "username/password pair for ssh"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC           = {
    "name"             : _ADAPTOR_NAME,
    "cfg_options"      : _ADAPTOR_OPTIONS, 
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "description"      : """ 
        The shell file adaptor. This adaptor uses the sh command line tools (sh,
        ssh, gsissh) to access remote filesystems.
        """,
    "details"          : """ 
        A more elaborate description....

        Known Limitations:
        ------------------

          * number of system pty's are limited:  each object bound
            to this adaptor will use 2 pairs of pty pipes.  Systems usually
            limit the number of available pty's to 1024 .. 4096.  Given that
            other processes also use pty's , that gives a hard limit to the number
            of object instances which can be created concurrently.  Hitting the
            pty limit will cause the following error message (or similar)::

              NoSuccess: pty_allocation or process creation failed (ENOENT: no more ptys)

            This limitation comes from saga.utils.pty_process.  On Linux
            systems, the utilization of pty's can be monitored::

               echo "allocated pty's: `cat /proc/sys/kernel/pty/nr`"
               echo "available pty's: `cat /proc/sys/kernel/pty/max`"


          * number of ssh connections are limited: sshd's default configuration,
            which is in place on many systems, limits the number of concurrent
            ssh connections to 10 per user -- beyond that, connections are
            refused with the following error::

              NoSuccess: ssh_exchange_identification: Connection closed by remote host

            As the communication with the ssh channel is unbuffered, the
            dropping of the connection will likely cause this error message to
            be lost.  Instead, the adaptor will just see that the ssh connection
            disappeared, and will issue an error message similar to this one::

              NoSuccess: read from pty process failed (Could not read line - pty process died)

 
          * Other system limits (memory, CPU, selinux, accounting etc.) apply as
            usual.

          
          * thread safety: it is safe to create multiple ``filesystem.*``
            instances to the same target host at a time -- they should not
            interfere with each other.

        """,
    "schemas"          : {"file"   :"use /bin/sh to access local filesystems", 
                          "local"  :"alias for file://", 
                          "ssh"    :"use ssh to access remote filesystems", 
                          "gsissh" :"use gsissh to access remote filesystems"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    "name"             : _ADAPTOR_NAME,
    "version"          : "v0.1",
    "schemas"          : _ADAPTOR_SCHEMAS,
    "cpis"             : [
        { 
        "type"         : "saga.filesystem.Directory",
        "class"        : "ShellDirectory"
        }, 
        { 
        "type"         : "saga.filesystem.File",
        "class"        : "ShellFile"
        }
    ]
}

###############################################################################
# The adaptor class

class Adaptor (saga.adaptors.cpi.base.AdaptorBase):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """


    # ----------------------------------------------------------------
    #
    def __init__ (self) :

        saga.adaptors.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.id_re = re.compile ('^\[(.*)\]-\[(.*?)\]$')
        self.opts  = self.get_config ()

        self.notifications = self.opts['enable_notifications'].get_value ()

    # ----------------------------------------------------------------
    #
    def sanity_check (self) :

        # FIXME: also check for gsissh

        pass



###############################################################################
#
class ShellDirectory (saga.adaptors.cpi.filesystem.Directory) :
    """ Implements saga.adaptors.cpi.filesystem.Directory """

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (ShellDirectory, self)
        self._cpi_base.__init__ (api, adaptor)


    # ----------------------------------------------------------------
    #
    def __del__ (self) :

        # FIXME: not sure if we should PURGE here -- that removes states which
        # might not be evaluated, yet.  Should we mark state evaluation
        # separately? 
        #   cmd_state () { touch $DIR/purgeable; ... }
        # When should that be done?
        self._logger.error ("adaptor dying...")
        self._logger.trace ()
    
        #     try :
        #       # if self.shell : self.shell.run_sync ("PURGE", iomode=None)
        #         if self.shell : self.shell.run_sync ("QUIT" , iomode=None)
        #     except :
        #         pass

        self.finalize (kill_shell=True)
    

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session) :
        """ Directory instance constructor """

        self.url     = url
        self.flags   = flags
        self.session = session

        # FIXME: get ssh Master connection from _adaptor dict
        self.shell = saga.utils.pty_shell.PTYShell (self.url, self.session, 
                                                    self._logger)

        self.shell.set_initialize_hook (self.initialize)
        self.shell.set_finalize_hook   (self.finalize)

        self.initialize ()

    # ----------------------------------------------------------------
    #
    def initialize (self) :

        # start the shell, find its prompt.  If that is up and running, we can
        # bootstrap our wrapper script, and then run commands etc.

        # -- now stage the shell wrapper script, and run it.  Once that is up
        # and running, we can requests job start / management operations via its
        # stdio.

        base = "$HOME/.saga/adaptors/shell_file"

        ret, out, _ = self.shell.run_sync ("mkdir -p %s" % base)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to prepare base dir (%s)(%s)" % (ret, out))


        # TODO: replace some constants in the script with values from config
        # files, such as 'timeout' or 'purge_on_quit' ...
        self.shell.stage_to_file (src = shell_wrapper._WRAPPER_SCRIPT, 
                                  tgt = "%s/wrapper.sh" % base)

        # we run the script.  In principle, we should set a new / different
        # prompt -- but, due to some strange and very unlikely coincidence, the
        # script has the same prompt as the previous shell... - go figure ;-)
        #
        # Note that we use 'exec' - so the script replaces the shell process.
        # Thus, when the script times out, the shell dies and the connection
        # drops -- that will free all associated resources, and allows for
        # a clean reconnect.
        # ret, out, _ = self.shell.run_sync ("exec sh %s/wrapper.sh" % base)
      
        # Well, actually, we do not use exec, as that does not give us good
        # feedback on failures (the shell just quits) -- so we replace it with
        # this poor-man's version...
        ret, out, _ = self.shell.run_sync ("/bin/sh -c '/bin/sh %s/wrapper.sh $$ && kill -9 $PPID' || false" \
                                        % (base))

        # either way, we somehow ran the script, and just need to check if it
        # came up all right...
        if  ret != 0 :
            raise saga.NoSuccess ("failed to run wrapper (%s)(%s)" % (ret, out))

        self._logger.debug ("got cmd prompt (%s)(%s)" % (ret, out.strip ()))


    # ----------------------------------------------------------------
    #
    def finalize (self, kill_shell = False) :

        if  kill_shell :
            if  self.shell :
                self.shell.finalize (True)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def open (self, url, flags) :

        adaptor_state = { "from_open" : True,
                          "cwd"       : self.url,
                          "url"       : url,
                          "flags"     : flags }

        return saga.filesystem.File (_adaptor=self._adaptor, _adaptor_state=adaptor_state)

    # ----------------------------------------------------------------
    @SYNC_CALL
    def get_url (self) :
        return self.url


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list (self, npat, flags):

        # FIXME: eval flags

        if  None == npat :
            npat = "*"

        ret, out, _ = self.shell.run_sync ("ls -d %s\n" % npat)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to list(): (%s)(%s)" \
                               % (ret, out))

        lines = filter (None, out.split ("\n"))
        self._logger.debug (lines)

        if lines[0] != "OK" :
            raise saga.NoSuccess ("failed to list(): (%s)" % (lines))
        del lines[0]

        self.entries = []
        for line in lines :
            # FIXME: convert to absolute URLs?
            self.entries.append (saga.Url (line.strip ()))

        return self.entries
   
   
###############################################################################
#
class ShellFile (saga.adaptors.cpi.filesystem.File) :
    """ Implements saga.adaptors.cpi.filesystem.File
    """
    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (ShellFile, self)
        self._cpi_base.__init__ (api, adaptor)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, url, flags, adaptor_state):

        if  'from_open' in adaptor_state and adaptor_state['from_open'] :
            # comes from job.service.create_job()
            self.url   = adaptor_state["url"] 
            self.cwd   = adaptor_state["cwd"]
            self.flags = adaptor_state["flags"]

        else :
            self.url   = url
          # self.cwd   = cwd # FIXME
            self.flags = flags
        
        return self.get_api ()


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url (self):
        return self.url

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_file (self, flags):
        # FIXME: eval flags
        return True


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

