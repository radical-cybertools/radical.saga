
__author__    = "Andre Merzky, Ole Weidner, Alexander Grill"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" shell based file adaptor implementation """

import radical.utils        as ru
import saga.utils.pty_shell as sups
import saga.utils.misc      as sumisc

import saga.adaptors.base
import saga.adaptors.cpi.filesystem

from   saga.filesystem.constants import *

import re
import os
import sys
import time
import pprint

SYNC_CALL  = saga.adaptors.cpi.decorators.SYNC_CALL
ASYNC_CALL = saga.adaptors.cpi.decorators.ASYNC_CALL

GO_DEFAULT_URL = "gsissh://cli.globusonline.org/"


# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "saga.adaptor.globus_online_file"
_ADAPTOR_SCHEMAS       = ["go+gsisftp", "go+gridftp"]
_ADAPTOR_OPTIONS       = [
    { 
    # fuck our config system!  I don't want these to be strings!  And its not
    # even using isinstance! :/
    'category'         : 'saga.adaptor.globus_online_file',
    'name'             : 'enable_notifications', 
    'type'             : str, 
    'default'          : 'None',
    'valid_options'    : ['True', 'False', 'None'],
    'documentation'    : '''Enable email notifications for all file transfers.
                            Note that 'True' and 'False' will result in
                            permanent changes to your GO notification settings.
                            'None' will leave your profile's settings upchanged.''',
    'env_variable'     : None
    }
]

# --------------------------------------------------------------------
# the adaptor capabilities & supported attributes
#
_ADAPTOR_CAPABILITIES  = {
    "metrics"          : [],
    "contexts"         : {"x509"     : "X509 proxy for Globus",
                          "userpass" : "username/password pair for GlobusOnline"}
}

# --------------------------------------------------------------------
# the adaptor documentation
#
_ADAPTOR_DOC           = {
    "name"             : _ADAPTOR_NAME,
    "cfg_options"      : _ADAPTOR_OPTIONS, 
    "capabilities"     : _ADAPTOR_CAPABILITIES,
    "description"      : """ 
        The globusonline file adaptor. This adaptor uses the GO file transfer
        service (https://www.globus.org/).
        """,
    "details"          : """ 
        """,
    "schemas"          : {"go+gsisftp" : "use globus online for gsisftp file transfer", 
                          "go+gridftp" : "use globus online for gridftp file transfer"
        }
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    "name"             : _ADAPTOR_NAME,
    "version"          : "v0.1",
    "schemas"          : _ADAPTOR_SCHEMAS,
    "cpis"             : [
        { 
        "type"         : "saga.namespace.Directory",
        "class"        : "GODirectory"
        }, 
        { 
        "type"         : "saga.namespace.Entry",
        "class"        : "GOFile"
        },
        { 
        "type"         : "saga.filesystem.Directory",
        "class"        : "GODirectory"
        }, 
        { 
        "type"         : "saga.filesystem.File",
        "class"        : "GOFile"
        }
    ]
}

################################################################################
# The adaptor class

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

        self.opts   = self.get_config (_ADAPTOR_NAME)
        self.notify = self.opts['enable_notifications'].get_value ()
        self.shells = dict ()  # keep go shells for each session

    # --------------------------------------------------------------------------
    #
    def sanity_check (self) :

        pass


    # --------------------------------------------------------------------------
    #
    def get_go_shell (self, session, go_url=None) :

        # this basically return a pty shell for 
        #
        #   gsissh username@cli.globusonline.org
        #
        # X509 contexts are prefered, but ssh contexts, userpass and myproxy can
        # also be used.  If the given url has username / password encoded, we
        # create an userpass context out of it and add it to the (copy of) the
        # session.

        sid = session._id

        if  not sid in self.shells :

            self.shells[sid] = dict ()

            if  not go_url :
                new_url = saga.Url (GO_DEFAULT_URL)
            else :
                new_url = saga.Url (go_url) # deep copy

            # create the shell.  
            shell = sups.PTYShell (new_url, session, self._logger, posix=False) 
            self.shells[sid]['shell'] = shell

            # confirm the user ID for this shell
            self.shells[sid]['user'] = None

            _, out, _ = shell.run_sync ('profile')

            for line in out.split ('\n') :
                if  'User Name:' in line :
                    self.shells[sid]['user'] = line.split (':', 2)[1].strip ()
                    self._logger.debug ("using account '%s'" % self.shells[sid]['user'])
                    break

            if  not self.shells[sid]['user'] :
                raise saga.NoSuccess ("Could not confirm user id")


            if  self.notify != 'None' :
                if  self.notify == 'True' :
                    self._logger.debug ("disable email notifications")
                    shell.run_sync ('profile -n on')
                else :
                    self._logger.debug ("enable email notifications")
                    shell.run_sync ('profile -n off')


            # for this fresh shell, we get the list of public endpoints.  That list
            # will contain the set of hosts we can potentially connect to.
            self.get_go_endpoint_list (session, shell, fetch=True)

          # pprint.pprint (self.shells)


        # we have the shell for sure by now -- return it!
        return self.shells[session._id]['shell']


    # ----------------------------------------------------------------
    #
    def get_go_endpoint_ids (self, session, url) :

        sid = session._id

        if  not sid in self.shells :
            raise saga.InocrrectState ("GO shell disconnected")

        schemas = [x for x in url.schema.split ('+') if x != 'go']
        ep_str  = "%s_%s" % ("_".join (schemas), url.host)
        ep_name = "%s#%s" % (self.shells[sid]['user'], ep_str)
        ep_url  = saga.Url ()

        ep_url.schema   = "+".join (schemas)
        ep_url.host     = url.host
        ep_url.port     = url.port

        return ep_str, ep_name, ep_url


    # ----------------------------------------------------------------
    #
    def get_path_spec (self, session, url, path=None, cwd_url=None, cwd_path=None) :

        # we assume that, whenever we request a path spec, we also want to use
        # it, and we thus register and activate the endpoint, if needed.

        sid = session._id

        if  not sid in self.shells :
            raise saga.InocrrectState ("GO shell disconnected")

        shell = self.shells[sid]['shell']
        url   = saga.Url (url)

        if  not path : 
            path = url.path

        if  not cwd_url :                    
            cwd_url  = saga.Url (url)

            if  not cwd_path : 
                cwd_path = '.'
        else :
            if  not cwd_path : 
                cwd_path = cwd_url.path

        if  not url.host   : url.host   = cwd_url.host
        if  not url.schema : url.schema = cwd_url.schema

        if  not url.host   : raise saga.BadParameter ('need host for GO ops')
        if  not url.schema : raise saga.BadParameter ('need schema for GO ops')

        ep_str, ep_name, ep_url = self.get_go_endpoint_ids (session, url)

        # if both URLs point into the same namespace, and if the given path is
        # not absolute, then expand it relative to the cwd_path (if it exists).
        # Otherwise it is left to the unmodified path.
        ps_path = path
        if  sumisc.url_is_compatible (cwd_url, url) :
            if  not path.startswith ('/') :
                if  cwd_path : 
                    ps_path = "%s/%s" % (cwd_path, path)

        # the pathspec is the concatenation of ps_host and ps_path by a colon
        ps = "%s:%s" % (ep_str, ps_path)


        # check if we know the endpoint in ep_str, and create/activate as needed
        ep = self.get_go_endpoint (session, shell, ep_url)

        return ps


    # ----------------------------------------------------------------
    #
    def get_go_endpoint (self, session, shell, url) :
        
        # for the given URL, derive the endpoint string.
        # FIXME: make sure that the endpoint is activated 
        ep_str, ep_name, ep_url = self.get_go_endpoint_ids (session, url)

        ep = self.get_go_endpoint_list (session, shell, ep_name, fetch=False)

        if  not ep :

            # if not, check if it was created meanwhile (fetch again)
            ep = self.get_go_endpoint_list (session, shell, ep_name, fetch=True)

            if  not ep :

                # if not, create it, activate it, and refresh all entries
                shell.run_sync ("endpoint-add %s -p %s" % (ep_name, ep_url))

                # refresh endpoint entries again
                ep = self.get_go_endpoint_list (session, shell, ep_name, fetch=True)

                if  not ep :
                    # something above must have failed ...
                    raise saga.NoSuccess ("endpoint initialization failed")

        # we have the endpoint now, for sure -- make sure its activated
        if  not ep['Credential Status'] == 'ACTIVE' :

            shell.run_sync ("endpoint-activate -g %s" % ep_name)

            # reload list to check status
            ep = self.get_go_endpoint_list (session, shell, ep_name, fetch=True)

            if  not ep['Credential Status'] == 'ACTIVE' :
                raise saga.AuthorizationFailed ("endpoint activation failed")

        return ep
        

    # ----------------------------------------------------------------
    #
    def get_go_endpoint_list (self, session, shell, ep_name=None, fetch=False) :

        # if fecth is True, query the shell for an updated endpoint list.
        # then check if the given ep_name is a known endpoint name, and if so,
        # return that entry -- otherwise return None.  If no ep_name is given,
        # and fetch is True, we thus simply refresh the internal list.

        self._logger.debug ("updating endpoint list (%s, %s)" % (ep_name, fetch))

        if  fetch :
            endpoints = dict ()
            name      = None

            _, out, _ = shell.run_sync ("endpoint-list -v")

            for line in out.split ('\n') :
                elems = line.split (':', 1)

                if len(elems) != 2 :
                    continue

                key = elems[0].strip ()
                val = elems[1].strip ()

                if  not key or not val :
                    continue

                if  key == "Name" :

                    # we now operate on a new entry -- initialize it
                    name = val
                    endpoints[name] = dict()
                    
                    # we make sure that some entries always exist, to simplify error
                    # checks
                    endpoints[name]['Name']              = name
                    endpoints[name]['Credential Status'] = None
                    endpoints[name]['Host(s)']           = None

                else :

                    if  name :
                        endpoints[name][key] = val

            # replace the ep info dist with the new one, to clean out old entries.
            self.shells[session._id]['endpoints'] = endpoints

        if  ep_name :
            # return the requested entry, or None
            return self.shells[session._id]['endpoints'].get (ep_name, None)


    # ----------------------------------------------------------------
    #
    def run_go_cmd (self, shell, cmd, mode='raise') :

        _, out, err = shell.run_sync (cmd)

        # see if the second line starts with 'Error'.  Note that this assumes
        # that the command only is one line...
        lines = out.split ('\n')

        if  len(lines) > 1 :
            if  lines[1].startswith ('Error:') :
                err = "%s\n%s" % (err, out)
                out = None

            else :
                # on success, we always remove the first line, as that is not
                # part of the output, really (this shell does not support 
                # 'stty -echo'...
                out = '\n'.join (lines[1:])

        # FIXME: a 'translate_exception' call would be useful here...
        if  mode == 'raise' and err :
            raise saga.NoSuccess ("Error in '%s': %s" % (cmd, err))

        return out, err


    # ----------------------------------------------------------------
    #
    def mkparents (self, session, shell, tgt_ps) :

        # GO does not support mkdir -p, so we need to split the dir into
        # elements and create one after the other, ignoring errors for already
        # existing elements.

        host_ps, path_ps = tgt_ps.split (':', 1)
        
        self._logger.info ('mkparents %s' % path_ps)

        if  path_ps.startswith ('/') : cur_path = ''
        else                         : cur_path = '.'

        error = dict()
        path_elems = filter (None, path_ps.split ('/'))

        for path_elem in path_elems :

            cur_path = "%s/%s" % (cur_path, path_elem)
            out, err = self.run_go_cmd (shell, "mkdir %s:%s" % (host_ps, cur_path), mode=None)

            if  err :
                error[cur_path] = err

        if  len(error) :

            # some mkdir gave an error.  Check if the error occured on the last
            # dir (the tgt), and if that is not a ignorable report that it
            # already exists -- anything else will raise an exception though...
            if  cur_path in error :
                if  not 'Path already exists' in error[cur_path] :
                    # report other errors
                    raise saga.NoSuccess ("Could not make dir hierarchy: %s" % str(error))


################################################################################
#
class GODirectory (saga.adaptors.cpi.filesystem.Directory) :
    """ Implements saga.adaptors.cpi.filesystem.Directory """

    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        _cpi_base = super  (GODirectory, self)
        _cpi_base.__init__ (api, adaptor)


    # ----------------------------------------------------------------
    #
    def __del__ (self) :

        pass



    # ----------------------------------------------------------------
    #
    def _is_valid (self) :

        if  not self.valid :
            raise saga.IncorrectState ("this instance was closed or removed")


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session) :
        """ Directory instance constructor """

        if  flags == None :
            flags = 0

        self.url      = saga.Url (url) # deep copy
        self.path     = url.path       # keep path separate
        self.url.path = None

        self.flags    = flags
        self.session  = session
        self.valid    = False # will be set by initialize

        self.initialize ()

        return self.get_api ()


    # ----------------------------------------------------------------
    #
    def initialize (self) :

        # GO shell got started, found its prompt.  Now, change
        # to the initial (or later current) working directory.

        self.shell = self._adaptor.get_go_shell        (self.session)
        self.ep    = self._adaptor.get_go_endpoint     (self.session, self.shell, self.url)
        self.ep_str, self.ep_name, self.ep_url = \
                     self._adaptor.get_go_endpoint_ids (self.session, self.url)
        self.valid = True

        ps = self.get_path_spec ()

        if  not self.ep :
            raise saga.badparameter ("invalid dir '%s': %s" % (ps, out))

        if  self.flags & saga.filesystem.CREATE_PARENTS :
            self._adaptor.mkparents (self.session, self.shell, ps)

        elif self.flags & saga.filesystem.CREATE :
            self._adaptor.run_go_cmd (self.shell, "mkdir '%s'" % ps)

        else :
            # this is as good an existence test as we can manage...
            self._adaptor.run_go_cmd (self.shell, "ls '%s'" % ps)


        self._logger.debug ("initialized directory %s" % self.url)


    # ----------------------------------------------------------------
    #
    def finalize (self, kill=False) :

        if  kill and self.shell :
            self.shell.finalize (True)
            self.shell = None

        self.valid = False


    # ----------------------------------------------------------------
    #
    def get_path_spec (self, url=None, path=None) :

        return self._adaptor.get_path_spec (session  = self.session, 
                                            url      = url, 
                                            path     = path,
                                            cwd_url  = self.url, 
                                            cwd_path = self.path)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def open (self, url, flags) :

        self._is_valid ()

        adaptor_state = { "from_open" : True,
                          "url"       : saga.Url (self.url),   # deep copy
                          "path"      : self.path}

        if  sumisc.url_is_relative (url) :
            url = sumisc.url_make_absolute (self.get_url (), url)

        return saga.filesystem.File (url=url, flags=flags, session=self.session, 
                                     _adaptor=self._adaptor, _adaptor_state=adaptor_state)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def open_dir (self, url, flags) :

        self._is_valid ()

        adaptor_state = { "from_open" : True,
                          "url"       : saga.Url (self.url),   # deep copy
                          "path"      : self.path}

        if  sumisc.url_is_relative (url) :
            url = sumisc.url_make_absolute (self.get_url (), url)

        return saga.filesystem.Directory (url=url, flags=flags, session=self.session, 
                                          _adaptor=self._adaptor, _adaptor_state=adaptor_state)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def change_dir (self, tgt, flags) :

        tgt_url = saga.Url (tgt)

        # FIXME: attempt to get new EP
        if  not sumisc.url_is_compatible (self.url, tgt_url) :
            raise saga.BadParameter ("target dir outside of namespace '%s': %s" \
                                  % (self.url, tgt_url))

        if sumisc.url_is_relative (tgt_url) :

            self.path = tgt_url.path

        else :
            self.url      = tgt_url
            self.path     = self.url.path
            self.url.path = None

        self.initialize ()

        self._logger.debug ("changed directory (%s)" % (tgt))


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def close (self, timeout=None):

        if  timeout :
            raise saga.BadParameter ("timeout for close not supported")

        self.finalize (kill=True)


    # ----------------------------------------------------------------
    @SYNC_CALL
    def get_url (self) :

        self._is_valid ()

        return saga.Url (self.url) # deep copy


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list (self, npat, flags):

        self._is_valid ()

        npat_ps  = self.get_path_spec (url=npat)
        out, err = self._adaptor.run_go_cmd (self.shell, "ls '%s'" % (npat_ps))
        lines    = filter (None, out.split ("\n"))
        self._logger.debug (lines)

        self.entries = []
        for line in lines :
            self.entries.append (saga.Url (line.strip ()))

        return self.entries
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy_self (self, tgt, flags):

        self._is_valid ()

        return self.copy (src_in=None, tgt_in=tgt, flags=flags)
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy (self, src_in, tgt_in, flags, _from_task=None):

        self._is_valid ()

        # FIXME: eval flags
        
        src_ps = self.get_path_spec (url=src_in)
        tgt_ps = self.get_path_spec (url=tgt_in)

        cmd_flags = ""
        if  flags & saga.filesystem.RECURSIVE : 
            cmd_flags  += "-r"

        if  flags & saga.filesystem.CREATE_PARENTS : 
            self._adaptor.mkparents (self.session, self.shell, tgt_ps)

        cmd      = "scp %s -D -s 0 '%s' '%s'" % (cmd_flags, src_ps, tgt_ps)
        out, err = self._adaptor.run_go_cmd (self.shell, cmd)

        # $ scp -r -v -s 0 -D user#ep_str:src user#ep_str:tgt
        # Task ID: be62038f-01ca-11e4-b57c-12313940394d
        tid = out.split (':', 1)[1].strip()

        out, err = self._adaptor.run_go_cmd (self.shell, "wait %s" % tid)


  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def link_self (self, tgt, flags):
  #
  #     self._is_valid ()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def link (self, src_in, tgt_in, flags, _from_task=None):
  #
  #     self._is_valid ()

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def move_self (self, tgt, flags):

        return self.move (src_in=None, tgt_in=tgt, flags=flags)
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def move (self, src_in, tgt_in, flags):

        # we handle move non-atomically, i.e. as copy/remove
        self.copy   (src_in, tgt_in, flags);
        self.remove (src_in, flags);
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove_self (self, flags):

        self._is_valid ()

        self.remove (self.url, flags)
        self.invalid = True
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove (self, tgt_in, flags):

        self._is_valid ()

        tgt_ps = self.get_path_spec (url=tgt_in)

        cmd_flags = ""
        if  flags & saga.filesystem.RECURSIVE : 
            cmd_flags  += "-r"

        cmd      = "rm %s -f -D '%s'" % (cmd_flags, tgt_ps)
        out, err = self._adaptor.run_go_cmd (self.shell, cmd)

        # $ tm -r -f -D user#ep_str:src user#ep_str:tgt
        # Task ID: be62038f-01ca-11e4-b57c-12313940394d
        tid = out.split (':', 1)[1].strip()

        out, err = self._adaptor.run_go_cmd (self.shell, "wait %s" % tid)
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def make_dir (self, tgt_in, flags):

        self._is_valid ()

        tgt_ps = self.get_path_spec (url=tgt_in)

        if  flags & saga.filesystem.CREATE_PARENTS : 
            self._adaptor.mkparents (self.session, self.shell, tgt_ps)

        else :
            cmd = "mkdir '%s'" % (cmd_flags, tgt_ps)
            self._adaptor.run_go_cmd (self.shell, cmd)

   
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_size_self (self) :
  #
  #     self._is_valid ()
  #
  #     return self.get_size (self.url)
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def get_size (self, tgt_in) :
  #
  #     self._is_valid ()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def is_dir_self (self):
  #
  #     self._is_valid ()
  #
  #     return self.is_dir (self.url)
  #
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def is_dir (self, tgt_in):
  #
  #     self._is_valid ()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def is_entry_self (self):
  #
  #     self._is_valid ()
  #
  #     return self.is_entry (self.url)
  #
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def is_entry (self, tgt_in):
  #
  #     self._is_valid ()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def is_link_self (self):
  #
  #     self._is_valid ()
  #
  #     return self.is_link (self.url)
  #
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def is_link (self, tgt_in):
  #
  #     self._is_valid ()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def is_file_self (self):
  #
  #     return self.is_entry_self ()
  #
  #
  # # ----------------------------------------------------------------
  # #
  # @SYNC_CALL
  # def is_file (self, tgt_in):
  #
  #     return self.is_entry (tgt_in)
   
   
###############################################################################
#
class GOFile (saga.adaptors.cpi.filesystem.File) :
    """ Implements saga.adaptors.cpi.filesystem.File
    """
    # ----------------------------------------------------------------
    #
    def __init__ (self, api, adaptor) :

        _cpi_base = super  (GOFile, self)
        _cpi_base.__init__ (api, adaptor)


    # ----------------------------------------------------------------
    #
    def __del__ (self) :

        self.finalize (kill=True)


    # ----------------------------------------------------------------
    #
    def _is_valid (self) :

        if  not self.valid :
            raise saga.IncorrectState ("this instance was closed or removed")


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session):

        # FIXME: eval flags!
        if  flags == None :
            flags = 0

        self._logger.info ("init_instance %s" % url)

        if  'from_open' in adaptor_state and adaptor_state['from_open'] :

            # comes from job.service.create_job()
            self.url         = saga.Url(url) # deep copy
            self.flags       = flags
            self.session     = session
            self.valid       = False  # will be set by initialize
            self.cwdurl      = saga.Url (adaptor_state["cwd"])
            self.cwd         = self.cwdurl.path

            if  sumisc.url_is_relative (self.url) :
                self.url = sumisc.url_make_absolute (self.cwd, self.url)

        else :

            if  sumisc.url_is_relative (url) :
                raise saga.BadParameter ("cannot interprete relative URL in this context ('%s')" % url)

            self.url         = url
            self.flags       = flags
            self.session     = session
            self.valid       = False  # will be set by initialize
            self.cwd         = sumisc.url_get_dirname (url)

            self.cwdurl      = saga.Url (url) # deep copy
            self.cwdurl.path = self.cwd


        # FIXME: get ssh Master connection from _adaptor dict
        self.shell = sups.PTYShell (self.url, self.session, self._logger)

      # self.shell.set_initialize_hook (self.initialize)
      # self.shell.set_finalize_hook   (self.finalize)

        self.initialize ()


        # we create a local shell handle, too, if only to support copy and move
        # to and from local file systems (mkdir for staging target, remove of move
        # source).  Not that we do not perform a cd on the local shell -- all
        # operations are assumed to be performed on absolute paths.
        self.local = sups.PTYShell ('fork://localhost/', saga.Session(default=True), 
                                    self._logger)

        return self.get_api ()


    # ----------------------------------------------------------------
    #
    def initialize (self) :

        # shell got started, found its prompt.  Now, change
        # to the initial (or later current) working directory.

        cmd = ""
        dirname = sumisc.url_get_dirname  (self.url)

        if  self.flags & saga.filesystem.CREATE_PARENTS :
            cmd = " mkdir -p '%s'; touch '%s'" % (dirname, self.url.path)
            self._logger.info ("mkdir '%s'; touch '%s'" % (dirname, self.url.path))

        elif self.flags & saga.filesystem.CREATE :
            cmd = " touch '%s'" % (self.url.path)
            self._logger.info ("touch %s" % self.url.path)

        else :
            cmd = " true"


        if  self.flags & saga.filesystem.READ :
            cmd += "; test -r '%s'" % (self.url.path)

        if  self.flags & saga.filesystem.WRITE :
            cmd += "; test -w '%s'" % (self.url.path)

        ret, out, _ = self.shell.run_sync (cmd)

        if  ret != 0 :
            if  self.flags & saga.filesystem.CREATE_PARENTS :
                raise saga.BadParameter ("cannot open/create: '%s' - %s" % (self.url.path, out))
            elif self.flags & saga.filesystem.CREATE :
                raise saga.BadParameter ("cannot open/create: '%s' - %s" % (self.url.path, out))
            else :
                raise saga.DoesNotExist("File does not exist: '%s' - %s" % (self.url.path, out))

        self._logger.info ("file initialized (%s)(%s)" % (ret, out))

        self.valid = True


    # ----------------------------------------------------------------
    #
    def finalize (self, kill=False) :

        if  kill and self.shell :
            self.shell.finalize (True)
            self.shell = None

        if  kill and self.local :
            self.local.finalize (True)
            self.local = None

        if  kill and self.copy_shell :
            self.copy_shell.finalize (True)
            self.copy_shell = None

        self.valid = False


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def close (self, timeout=None):

        if  timeout :
            raise saga.BadParameter ("timeout for close not supported")

        self.finalize (kill=True)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url (self):

        self._is_valid ()

        return saga.Url (self.url) # deep copy


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy_self (self, tgt_in, flags):

        self._is_valid ()

        # FIXME: eval flags

        # print "copy self %s -> %s" % (self.url, tgt_in)

        cwdurl = saga.Url (self.cwdurl) # deep copy
        src    = saga.Url (self.url)    # deep copy
        tgt    = saga.Url (tgt_in)      # deep copy

        if sumisc.url_is_relative (src) : src = sumisc.url_make_absolute (cwdurl, src)
        if sumisc.url_is_relative (tgt) : tgt = sumisc.url_make_absolute (cwdurl, tgt)

        rec_flag = ""
        if  flags & saga.filesystem.RECURSIVE : 
            rec_flag  += "-r "

        if  flags & saga.filesystem.CREATE_PARENTS : 
            self._create_parent (cwdurl, tgt)

        # if cwd, src and tgt point to the same host, we just run a shell cp
        # command on that host
        if  sumisc.url_is_compatible (cwdurl, src) and \
            sumisc.url_is_compatible (cwdurl, tgt) :

            # print "shell cp"
            ret, out, _ = self.shell.run_sync (" cp %s '%s' '%s'\n" % (rec_flag, src.path, tgt.path))
            if  ret != 0 :
                raise saga.NoSuccess ("copy (%s -> %s) failed (%s): (%s)" \
                                   % (src, tgt, ret, out))


        # src and tgt are on different hosts, we need to find out which of them
        # is local (stage_from vs. stage_to).
        else :
            # print "! shell cp"

            # if cwd is remote, we use stage from/to on the existing pipe
            if  not sumisc.url_is_local (cwdurl) :

                # print "cwd remote"

                if  sumisc.url_is_local (src)          and \
                    sumisc.url_is_compatible (cwdurl, tgt) :

                    # print "from local to remote"
                    files_copied = self.shell.stage_to_remote (src.path, tgt.path, rec_flag)

                elif sumisc.url_is_local (tgt)          and \
                     sumisc.url_is_compatible (cwdurl, src) :

                    # print "from remote to loca"
                    files_copied = self.shell.stage_from_remote (src.path, tgt.path, rec_flag)

                else :
                    # print "from remote to other remote -- fail"
                    # we cannot support the combination of URLs
                    raise saga.BadParameter ("copy from %s to %s is not supported" \
                                          % (src, tgt))
   

            # if cwd is local, and src or tgt are remote, we need to actually
            # create a new pipe to the target host.  note that we may not have
            # a useful session for that!
            else : # sumisc.url_is_local (cwdurl) :

                # print "cwd local"

                if  sumisc.url_is_local (src) :

                    # need a compatible target scheme
                    if  tgt.scheme and not tgt.scheme.lower () in _ADAPTOR_SCHEMAS :
                        raise saga.BadParameter ("schema of copy target is not supported (%s)" \
                                              % (tgt))

                    # print "from local to remote"
                    copy_shell   = self._get_copy_shell (tgt)
                    files_copied = copy_shell.stage_to_remote (src.path, tgt.path, rec_flag)

                elif sumisc.url_is_local (tgt) :

                    # need a compatible source scheme
                    if  src.scheme and not src.scheme.lower () in _ADAPTOR_SCHEMAS :
                        raise saga.BadParameter ("schema of copy source is not supported (%s)" \
                                              % (src))

                    # print "from remote to local"
                    copy_shell   = self._get_copy_shell (tgt)
                    files_copied = copy_shell.stage_from_remote (src.path, tgt.path, rec_flag)

                else :

                    # we cannot support two remote URLs
                    raise saga.BadParameter ("copy from %s to %s is not supported" \
                                          % (src, tgt))

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def link_self (self, tgt_in, flags, _from_task=None):

        # link will *only* work if src and tgt are on the same resource (and
        # even then may fail)
        self._is_valid ()

        cwdurl = saga.Url (self.url) # deep copy
        src    = saga.Url (self.url)    # deep copy
        tgt    = saga.Url (tgt_in)   # deep copy

        rec_flag = ""
        if  flags & saga.filesystem.RECURSIVE : 
            raise saga.BadParameter ("'RECURSIVE' flag not  supported for link()")

        if  flags & saga.filesystem.CREATE_PARENTS : 
            self._create_parent (cwdurl, tgt)

        # if src and tgt point to the same host, we just run a shell link
        # on that host
        if  sumisc.url_is_compatible (cwdurl, src) and \
            sumisc.url_is_compatible (cwdurl, tgt) :

            # print "shell ln"
            ret, out, err = self.shell.run_sync (" ln -s '%s' '%s'\n" % (src.path, tgt.path))
            if  ret != 0 :
                raise saga.NoSuccess ("link (%s -> %s) failed (%s): (out: %s) (err: %s)" \
                                   % (src, tgt, ret, out, err))


        # src and tgt are on different hosts, this is not supported
        else :
            raise saga.BadParameter ("link is only supported on same file system as cwd")



    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def move_self (self, tgt_in, flags):

        # we handle move non-atomically, i.e. as copy/remove
        self.copy_self   (tgt_in, flags)
        self.remove_self (flags)

        # however, we are not closed at this point, but need to re-initialize
        self.url   = tgt_in
        self.flags = flags
        self.initialize ()

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def write (self, string, flags=None):
	"""
	This call is intended to write a string to a local or remote file.
	Since write() uses file staging calls, it cannot be used to randomly
	write certain parts of a file (i.e. seek()). Together with read(),
	it was designed to manipulate template files and write them back to
	the remote directory. Be aware, that writing large files will
	be very slow compared to native read(2) and write(2) calls.
	"""
        self._is_valid ()
        if  flags==None:
            flags = self.flags
        else:
            self.flags=flags

        tgt = saga.Url (self.url)  # deep copy, is absolute
            
        if  flags==saga.filesystem.APPEND:
            string = self.read()+string            
        # FIXME: eval flags

        self.shell.write_to_remote(string,tgt.path)
                                                    
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def read (self,size=None):
	"""
	This call is intended to read a string wit length size from a local
	or remote file.	Since read() uses file staging calls, it cannot be
	used to randomly read certain parts of a file (i.e. seek()).
	Together with write(), it was designed to manipulate template files
	and write them back to the remote directory. Be aware, that reading
	large files will be very slow compared to native read(2) and write(2)
	calls.
	"""

        self._is_valid ()

        tgt = saga.Url (self.url)  # deep copy, is absolute
        
        out = self.shell.read_from_remote(tgt.path)

        if  size!=None:
            return out[0:size-1]
        else:
            return out


   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove_self (self, flags):

        self._is_valid ()

        # FIXME: eval flags
        # FIXME: check if tgt remove happens to affect cwd... :-P

        tgt = saga.Url (self.url)  # deep copy, is absolute

        rec_flag = ""
        if  flags & saga.filesystem.RECURSIVE : 
            rec_flag  += "-r "

        ret, out, _ = self.shell.run_sync (" rm -f %s '%s'\n" % (rec_flag, tgt.path))
        if  ret != 0 :
            raise saga.NoSuccess ("remove (%s) failed (%s): (%s)" \
                               % (tgt, ret, out))

   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_size_self (self) :

        self._is_valid ()
        size      = None
        size_mult = 1
        ret       = None
        out       = None

        if  self.is_dir_self () :
            size_mult   = 1024   # see '-k' option to 'du'
            ret, out, _ = self.shell.run_sync (" du -ks '%s'  | xargs | cut -f 1 -d ' '\n" \
                                            % self.url.path)
        else :
            ret, out, _ = self.shell.run_sync (" wc -c '%s' | xargs | cut -f 1 -d ' '\n" \
                                            % self.url.path)

        if  ret != 0 :
            raise saga.NoSuccess ("get size for (%s) failed (%s): (%s)" \
                               % (self.url, ret, out))

        try :
            size = int (out) * size_mult
        except Exception as e :
            raise saga.NoSuccess ("could not get file size: %s" % out)


        return size
   

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_dir_self (self):

        self._is_valid ()

        cwdurl = saga.Url (self.url) # deep copy

        ret, out, _ = self.shell.run_sync (" test -d '%s' && test ! -h '%s'" % (cwdurl.path, cwdurl.path))

        return True if ret == 0 else False


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_entry_self (self):

        self._is_valid ()

        cwdurl = saga.Url (self.url) # deep copy

        ret, out, _ = self.shell.run_sync (" test -f '%s' && test ! -h '%s'" % (cwdurl.path, cwdurl.path))

        return True if ret == 0 else False
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_link_self (self):

        self._is_valid ()

        cwdurl = saga.Url (self.url) # deep copy

        ret, out, _ = self.shell.run_sync (" test -h '%s'" % cwdurl.path)

        return True if ret == 0 else False
   
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_file_self (self):

        self._is_valid ()

        cwdurl = saga.Url (self.url) # deep copy

        ret, out, _ = self.shell.run_sync (" test -f '%s' && test ! -h '%s'" % (cwdurl.path, cwdurl.path))

        return True if ret == 0 else False
   
   



