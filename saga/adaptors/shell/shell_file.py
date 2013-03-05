
""" shell based file adaptor implementation """

import saga.utils.pty_shell as sups
import saga.utils.misc      as sumisc

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

        self.finalize (kill=True)
    

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session) :
        """ Directory instance constructor """

        self.url     = saga.Url (url) # deep copy
        self.flags   = flags
        self.session = session

        # cwd is where this directory is in, so the path w/o the last element
        path = self.url.path.rstrip ('/')
        self.cwd     = sumisc.url_get_dirname (path)
        self.cwdurl  = saga.Url (url)
        self.cwdurl.path = self.cwd

        # FIXME: get ssh Master connection from _adaptor dict
        self.shell = sups.PTYShell     (self.url, self.session, self._logger)

        self.shell.set_initialize_hook (self.initialize)
        self.shell.set_finalize_hook   (self.finalize)

        self.initialize ()

        return self.get_api ()

    # ----------------------------------------------------------------
    #
    def initialize (self) :

        # shell git started, found its prompt.  Now, change
        # to the initial (or later current) working directory.

        ret, out, _ = self.shell.run_sync ("cd %s" % self.url.path)

        if  ret != 0 :
            raise saga.BadParameter ("invalid dir '%s': %s" % (self.url.path, out))

        self._logger.debug ("initialized directory (%s)(%s)" % (ret, out))


    # ----------------------------------------------------------------
    #
    def finalize (self, kill = False) :

        if  kill and self.shell :
            self.shell.finalize (True)
            self.shell = None


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def open (self, url, flags) :

        # NOTE:
        # In principle, we could also pass the self.shell here, to re-use
        # existing connections.  However, that would imply:
        #   - strictly keeping cwd on adaptor side, and always compute complete
        #     paths here
        #   - ensure thread safety accross API objects
        #   - ensure shell lifetime management
        # This seems not worth the tradeoff until we hit connection and session
        # limit too frequently...

        adaptor_state = { "from_open" : True,
                          "cwd"       : self.url }

        if sumisc.url_is_relative (url) :
            url = sumisc.url_make_absolute (self.get_url (), url)

        return saga.filesystem.File (url=url, flags=flags, session=self.session, 
                                     _adaptor=self._adaptor, _adaptor_state=adaptor_state)

    # ----------------------------------------------------------------
    @SYNC_CALL
    def get_url (self) :

        return saga.Url (self.url) # deep copy


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list (self, npat, flags):

        # FIXME: eval flags

        if  None == npat :
            npat = "*"

        ret, out, _ = self.shell.run_sync ("/bin/ls -C1 -d %s\n" % npat)
        if  ret != 0 :
            raise saga.NoSuccess ("failed to list(): (%s)(%s)" \
                               % (ret, out))

        lines = filter (None, out.split ("\n"))
        self._logger.debug (lines)

        self.entries = []
        for line in lines :
            # FIXME: convert to absolute URLs?
            self.entries.append (saga.Url (line.strip ()))

        return self.entries
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy_self (self, tgt, flags):

        # FIXME: eval flags
        src = self.get_url ()
        return self.copy (src, tgt, flags)
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy (self, src_in, tgt_in, flags):

        # FIXME: eval flags
        
        cwd = self.url

        src = saga.Url (src_in)  # deep copy
        tgt = saga.Url (tgt_in)  # deep copy

        if sumisc.url_is_relative (src) : src = sumisc.url_make_absolute (cwd, src)
        if sumisc.url_is_relative (tgt) : tgt = sumisc.url_make_absolute (cwd, tgt)

        copy_flags = ""

        if flags & saga.filesystem.RECURSIVE      : copy_flags  += "-r "
        if flags & saga.filesystem.CREATE_PARENTS : mkdir_flags += "-p "


        # if cwd, src and tgt point to the same host, we just run a shell cp
        # command on that host
        if  sumisc.url_is_compatible (cwd, src) and \
            sumisc.url_is_compatible (cwd, tgt) :

            ret, out, _ = self.shell.run_sync ("cp %s %s %s\n" % (copy_flags, src.path, tgt.path))
            if  ret != 0 :
                raise saga.NoSuccess ("Copy (%s -> %s) failed (%s): (%s)" \
                                   % (src, tgt, ret, out))
            return


        # if cwd and src are on the same host, and tgt is local, we stage from
        # that host to local
        if  sumisc.url_is_compatible (cwd, src) :

            return self.shell.stage_from_file (tgt, src, copy_flags)

        # if cwd and tgt are on the same host, and src is local, we stage from
        # local to the target host
        if  sumisc.url_is_compatible (cwd, src) :

            return self.shell.stage_to_file (src, tgt, copy_flags)

        # we cannot support the combination of URLs
        raise saga.BadParameter ("copy from %s to %s is not supported" \
                                 % (src, tgt))
   

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_size (self, tgt_in) :

        tgt = saga.Url (tgt_in)

        tgt_abs = sumisc.url_make_absolute (self.cwdurl, tgt)

        ret, out, _ = self.shell.run_sync ("wc -c %s  | cut -f 1 -d ' '\n" % tgt.path)
        if  ret != 0 :
            raise saga.NoSuccess ("get size for (%s) failed (%s): (%s)" \
                               % (tgt, ret, out))

        size = int (out)
        return size
   
   
   
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
    def init_instance (self, adaptor_state, url, flags, session):

        # FIXME: eval flags!

        if  'from_open' in adaptor_state and adaptor_state['from_open'] :
            # comes from job.service.create_job()
            self.url     = url
            self.flags   = flags
            self.session = session
            self.cwd     = adaptor_state["cwd"]

            if  sumisc.url_is_relative :
                self.url = sumisc.url_make_absolute (self.cwd, self.url)

        else :
            self.url   = url
            self.flags = flags
            self.cwd   = sumisc.url_get_dirname (url)

        # FIXME: get ssh Master connection from _adaptor dict
        self.shell = sups.PTYShell (self.url, self.session, self._logger)

        self.shell.set_initialize_hook (self.initialize)
        self.shell.set_finalize_hook   (self.finalize)

        self.initialize ()

        return self.get_api ()


    # ----------------------------------------------------------------
    #
    def initialize (self) :

        # shell git started, found its prompt.  Now, change
        # to the initial (or later current) working directory.

        ret, out, _ = self.shell.run_sync ("test -e %s" % self.url.path)

        if  ret != 0 :
            raise saga.BadParameter ("file does not exist: '%s' - %s" % (self.url.path, out))

        self._logger.debug ("file initialized (%s)(%s)" % (ret, out))


    # ----------------------------------------------------------------
    #
    def finalize (self, kill = False) :

        if  kill and self.shell :
            self.shell.finalize (True)
            self.shell = None


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

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy_self (self, tgt_in, flags):

        # FIXME: eval flags

        cwd = self.cwd

        src = saga.Url (self.url)  # deep copy
        tgt = saga.Url (tgt_in)    # deep copy

        if sumisc.url_is_relative (src) : src = sumisc.url_make_absolute (cwd, src)
        if sumisc.url_is_relative (tgt) : tgt = sumisc.url_make_absolute (cwd, tgt)

        copy_flags = ""

        if flags & saga.filesystem.CREATE_PARENTS : mkdir_flags += "-p "


        # if cwd, src and tgt point to the same host, we just run a shell cp
        # command on that host
        if  sumisc.url_is_compatible (src, cwd) and \
            sumisc.url_is_compatible (tgt, cwd) :

            ret, out, _ = self.shell.run_sync ("cp %s %s %s\n" % (copy_flags, src.path, tgt.path))
            if  ret != 0 :
                raise saga.NoSuccess ("Copy (%s -> %s) failed (%s): (%s)" \
                                   % (src, tgt, ret, out))
            return


        # if cwd and src are on the same host, and tgt is local, we stage from
        # that host to local
        if  sumisc.url_is_compatible (src, cwd) :

            return self.shell.stage_from_file (tgt, src, copy_flags)

        # if cwd and tgt are on the same host, and src is local, we stage from
        # local to the target host
        if  sumisc.url_is_compatible (src, cwd) :

            return self.shell.stage_to_file (src, tgt, copy_flags)

        # we cannot support the combination of URLs
        raise saga.BadParameter ("copy from %s to %s is not supported" \
                                 % (src, tgt))
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_size (self) :

        ret, out, _ = self.shell.run_sync ("wc -c %s  | cut -f 1 -d ' '\n" % self.url.path)
        if  ret != 0 :
            raise saga.NoSuccess ("get size for (%s) failed (%s): (%s)" \
                               % (tgt, ret, out))

        size = int (out)
        return size
   
   
   

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

