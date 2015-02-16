
__author__    = "Andre Merzky, Mark Santcroos"
__copyright__ = "Copyright 2014-2015, The SAGA Project"
__license__   = "MIT"


""" (GSI)SSH based Globus Online Adaptor """

import saga.utils.pty_shell as sups
import saga.utils.misc      as sumisc

import saga.adaptors.base
import saga.adaptors.cpi.filesystem

from saga.adaptors.cpi.decorators import SYNC_CALL

GO_DEFAULT_URL = "gsissh://cli.globusonline.org/"

# --------------------------------------------------------------------
# the adaptor name
#
_ADAPTOR_NAME          = "saga.adaptor.globus_online_file"
_ADAPTOR_SCHEMAS       = ["go"]
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
    },
    { 
    # fuck our config system!  I don't want these to be strings!  And its not
    # even using isinstance! :/
    'category'         : 'saga.adaptor.globus_online_file',
    'name'             : 'failure_mode', 
    'type'             : str, 
    'default'          : 'report',
    'valid_options'    : ['raise', 'report', 'ignore'],
    'documentation'    : '''Globus-Online seems to behave eratically.  This flag
                         defines how the adaptor should deal with intermittent
                         and fatal errors.  'raise' will cause exceptions on
                         all errors, 'report' will print error messages, but
                         otherwise continue, and 'ignore' will (duh!) ignore
                         errors.  'report' is the default, you should only use
                         'ignore' when you know what you are doing!''',
    'env_variable'     : None
    },
    # Set a GO specific prompt on the PTYShell
    {
        'category'      : 'saga.adaptor.globus_online_file',
        'name'          : 'prompt_pattern',
        'type'          : str,
        'default'       : '\$ $',
        'documentation' : 'GO always has a "$ " prompt',
        'env_variable'  : None
    },
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
    "schemas"          : {"go": "use globus online for gridftp file transfer"}
}

# --------------------------------------------------------------------
# the adaptor info is used to register the adaptor with SAGA

_ADAPTOR_INFO          = {
    "name"             : _ADAPTOR_NAME,
    "version"          : "v0.2",
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

class Adaptor(saga.adaptors.base.Base):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """

    # --------------------------------------------------------------------------
    #
    def __init__(self):

        saga.adaptors.base.Base.__init__(self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.opts   = self.get_config(_ADAPTOR_NAME)
        self.notify = self.opts['enable_notifications'].get_value()
        self.f_mode = self.opts['failure_mode'].get_value()
        self.prompt = self.opts['prompt_pattern'].get_value()
        self.shells = {}  # keep go shells for each session

    # --------------------------------------------------------------------------
    #
    def sanity_check(self):
        pass

    # --------------------------------------------------------------------------
    #
    def get_go_shell(self, session, go_url=None):

        # this basically return a pty shell for 
        #
        #   gsissh username@cli.globusonline.org
        #
        # X509 contexts are preferred, but ssh contexts, userpass and myproxy can
        # also be used.  If the given url has username / password encoded, we
        # create an userpass context out of it and add it to the (copy of) the
        # session.

        sid = session._id

        if not sid in self.shells:

            self.shells[sid] = {}

            if not go_url:
                new_url = saga.Url(GO_DEFAULT_URL)
            else:
                new_url = saga.Url(go_url) # deep copy

            opts = {'prompt_pattern' : self.prompt}

            # create the shell.
            shell = sups.PTYShell(new_url, session=session, logger=self._logger, 
                                  options=opts, posix=False)
            self.shells[sid]['shell'] = shell

            # confirm the user ID for this shell
            self.shells[sid]['user'] = None

            _, out, _ = shell.run_sync('profile')

            for line in out.split('\n'):
                if 'User Name:' in line:
                    self.shells[sid]['user'] = line.split(':', 2)[1].strip()
                    self._logger.debug("using account '%s'" % self.shells[sid]['user'])
                    break

            if not self.shells[sid]['user']:
                raise saga.NoSuccess("Could not confirm user id")

            if self.notify != 'None':
                if self.notify == 'True':
                    self._logger.debug("disable email notifications")
                    shell.run_sync('profile -n on')
                else:
                    self._logger.debug("enable email notifications")
                    shell.run_sync('profile -n off')

            # for this fresh shell, we get the list of public endpoints.  That list
            # will contain the set of hosts we can potentially connect to.
            self.get_go_endpoint_list(session, shell, fetch=True)

          # pprint.pprint(self.shells)


        # we have the shell for sure by now -- return it!
        return self.shells[session._id]['shell']

    # ----------------------------------------------------------------
    #
    def get_go_endpoint_ids(self, session, url):

        sid = session._id

        if not sid in self.shells:
            raise saga.IncorrectState("GO shell disconnected")

        if '#' in url.host:
            ep_name = url.host
        else:
            ep_name = "%s#%s" % (self.shells[sid]['user'], url.host)
        ep_url  = saga.Url()

        ep_url.schema   = url.schema
        ep_url.host     = url.host
        ep_url.port     = url.port

        return url.host, ep_name, ep_url

    # ----------------------------------------------------------------
    #
    def get_path_spec(self, session, url, path=None, cwd_url=None, cwd_path=None):

        # we assume that, whenever we request a path spec, we also want to use
        # it, and we thus register and activate the endpoint, if needed.

        sid = session._id

        if not sid in self.shells:
            raise saga.IncorrectState("GO shell disconnected")

        shell = self.shells[sid]['shell']
        url   = saga.Url(url)

        if not path:
            path = url.path

        if not cwd_url:
            cwd_url = saga.Url(url)

            if not cwd_path:
                cwd_path = '.'
        else:
            if not cwd_path:
                cwd_path = cwd_url.path

        if not url.host:
            url.host   = cwd_url.host
        if not url.schema:
            url.schema = cwd_url.schema

        if not url.host:
            raise saga.BadParameter('need host for GO ops')
        if not url.schema:
            raise saga.BadParameter('need schema for GO ops')

        ep_str, ep_name, ep_url = self.get_go_endpoint_ids(session, url)

        # if both URLs point into the same namespace, and if the given path is
        # not absolute, then expand it relative to the cwd_path (if it exists).
        # Otherwise it is left to the unmodified path.
        ps_path = path
        if sumisc.url_is_compatible(cwd_url, url):
            if not path.startswith('/'):
                if cwd_path:
                    ps_path = "%s/%s" % (cwd_path, path)

        # the pathspec is the concatenation of ps_host and ps_path by a colon
        ps = "%s:%s" % (ep_str, ps_path)

        # check if we know the endpoint in ep_str, and create/activate as needed
        ep = self.get_go_endpoint(session, shell, ep_url)

        return ps

    # ----------------------------------------------------------------
    #
    def get_go_endpoint(self, session, shell, url):

        # for the given URL, derive the endpoint string.
        # FIXME: make sure that the endpoint is activated 
        ep_str, ep_name, ep_url = self.get_go_endpoint_ids(session, url)

        ep = self.get_go_endpoint_list(session, shell, ep_name, fetch=False)

        if not ep:

            # if not, check if it was created meanwhile (fetch again)
            ep = self.get_go_endpoint_list(session, shell, ep_name, fetch=True)

            if not ep:

                # Don't try to create endpoints that are supposed to be there
                # (plus, we can't!)
                if '#' in url.host:
                    raise saga.NoSuccess("# in hostname, not going to create!")

                # if not, create it, activate it, and refresh all entries
                shell.run_sync("endpoint-add %s -p %s" % (ep_name, ep_url))

                # refresh endpoint entries again
                ep = self.get_go_endpoint_list(session, shell, ep_name, fetch=True)

                if not ep:
                    # something above must have failed ...
                    raise saga.NoSuccess("endpoint initialization failed")

        # we have the endpoint now, for sure -- make sure its activated
        if not ep['Credential Status'] == 'ACTIVE':

            shell.run_sync("endpoint-activate -g %s" % ep_name)

            # reload list to check status
            ep = self.get_go_endpoint_list(session, shell, ep_name, fetch=True)

            if not ep['Credential Status'] == 'ACTIVE':
                raise saga.AuthorizationFailed("endpoint activation failed")

        return ep
        
    # ----------------------------------------------------------------
    #
    def get_go_endpoint_list(self, session, shell, ep_name=None, fetch=False):

        # if 'fetch' is True, query the shell for an updated endpoint list.
        # then check if the given ep_name is a known endpoint name, and if so,
        # return that entry -- otherwise return None.  If no ep_name is given,
        # and fetch is True, we thus simply refresh the internal list.

        if fetch:
            endpoints = {}
            name = None

            _, out, _ = shell.run_sync("endpoint-list -v")

            for line in out.split('\n'):
                elems = line.split(':', 1)

                if len(elems) != 2:
                    continue

                key = elems[0].strip()
                val = elems[1].strip()

                if not key or not val:
                    continue

                if key == "Name":

                    # we now operate on a new entry -- initialize it
                    name = val

                    endpoints[name] = {}
                    
                    # we make sure that some entries always exist, to simplify error
                    # checks
                    endpoints[name]['Name']              = name
                    endpoints[name]['Credential Status'] = None
                    endpoints[name]['Host(s)']           = None

                else:

                    # Continued passed of an entry, name should always exist
                    try:
                        endpoints[name][key] = val
                    except:
                        raise saga.NoSuccess("No entry name to operate on")

            # replace the ep info dist with the new one, to clean out old entries.
            self.shells[session._id]['endpoints'] = endpoints

        if ep_name:
            # return the requested entry, or None
            return self.shells[session._id]['endpoints'].get(ep_name, None)

    # ----------------------------------------------------------------
    #
    def run_go_cmd(self, shell, cmd, mode=None):

        # available modes:
        #   raise : raise NoSuccess on error
        #   report: print error message, but continue
        #   ignore: do nothing

        if not mode:
            mode = self.f_mode

        _, out, err = shell.run_sync(cmd)

        # see if the second line starts with 'Error'.  Note that this assumes
        # that the command only is one line...
        lines = out.split('\n')

        if len(lines) > 1:
            if lines[1].startswith('Error:'):
                err = "%s\n%s" % (err, '\n'.join(lines))
                out = None

            else:
                # on success, we always remove the first line, as that is not
                # part of the output, really (this shell does not support 
                # 'stty -echo'...
                out = '\n'.join(lines[1:])

        if err:
            if mode == 'raise':
                # FIXME: a 'translate_exception' call would be useful here...
                raise saga.NoSuccess("Error in '%s': %s" % (cmd, err))

            if mode == 'report':
                self._logger.error("Error in '%s': %s" % (cmd, err))

            if mode == 'silent':
                pass

        return out, err

    # ----------------------------------------------------------------
    #
    def mkparents(self, session, shell, tgt_ps):

        # GO does not support mkdir -p, so we need to split the dir into
        # elements and create one after the other, ignoring errors for already
        # existing elements.

        host_ps, path_ps = tgt_ps.split(':', 1)
        
        self._logger.info('mkparents %s' % path_ps)

        if path_ps.startswith('/'):
            cur_path = ''
        else:
            cur_path = '.'

        error = {}
        path_elems = filter(None, path_ps.split('/'))

        for path_elem in path_elems :

            cur_path = "%s/%s" % (cur_path, path_elem)
            out, err = self.run_go_cmd(shell, "mkdir %s:%s" % (host_ps, cur_path))

            if err:
                error[cur_path] = err

        if len(error):

            # some mkdir gave an error.  Check if the error occured on the last
            # dir (the tgt), and if that is not a ignorable report that it
            # already exists -- anything else will raise an exception though...
            if cur_path in error:

                if not 'Path already exists' in error[cur_path]:

                    if self.f_mode == 'raise':
                        # FIXME: a 'translate_exception' call would be useful here...
                        raise saga.NoSuccess("Could not make dir hierarchy: %s" % str(error))

                    if self.f_mode == 'report':
                        self._logger.error("Could not make dir hierarchy: %s" % str(error))

                    if self.f_mode == 'silent':
                        pass


################################################################################
#
class GODirectory(saga.adaptors.cpi.filesystem.Directory):
    """ Implements saga.adaptors.cpi.filesystem.Directory """

    # ----------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        _cpi_base = super(GODirectory, self)
        _cpi_base.__init__(api, adaptor)

    # ----------------------------------------------------------------
    #
    def _is_valid(self):

        if not self.valid:
            raise saga.IncorrectState("this instance was closed or removed")

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, url, flags, session):
        """ Directory instance constructor """

        # FIXME: eval flags!
        if flags == None:
            flags = 0

        self.orig     = saga.Url(url) # deep copy
        self.url      = saga.Url(url) # deep copy
        self.path     = url.path       # keep path separate
        self.url.path = None

        self.flags    = flags
        self.session  = session
        self.valid    = False # will be set by initialize

        self.initialize()

        return self.get_api()

    # ----------------------------------------------------------------
    #
    def initialize(self):

        # GO shell got started, found its prompt.  Now, change
        # to the initial (or later current) working directory.

        self.shell = self._adaptor.get_go_shell(self.session)
        self.ep    = self._adaptor.get_go_endpoint(self.session, self.shell, self.url)
        self.ep_str, self.ep_name, self.ep_url = \
                     self._adaptor.get_go_endpoint_ids(self.session, self.url)

        ps = self.get_path_spec()

        if not self.ep:
            raise saga.BadParameter("invalid dir '%s'" % ps)

        if self.flags & saga.filesystem.CREATE_PARENTS:
            self._adaptor.mkparents(self.session, self.shell, ps)

        elif self.flags & saga.filesystem.CREATE:
            self._adaptor.run_go_cmd(self.shell, "mkdir '%s'" % ps)

        else:
            # this is as good an existence test as we can manage...
            self._adaptor.run_go_cmd(self.shell, "ls '%s'" % ps)

        self._logger.debug("Initialized directory %s/%s" % (self.url, self.path))

        self.valid = True

    # ----------------------------------------------------------------
    #
    def finalize(self, kill=False):

        if kill and self.shell:
            self.shell.finalize(True)
            self.shell = None

        self.valid = False

    # ----------------------------------------------------------------
    #
    def get_path_spec(self, url=None, path=None):

        return self._adaptor.get_path_spec(session  = self.session,
                                            url      = url, 
                                            path     = path,
                                            cwd_url  = self.url, 
                                            cwd_path = self.path)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def open(self, url, flags):

        self._is_valid()

        adaptor_state = { "from_open" : True,
                          "url"       : saga.Url(self.url),   # deep copy
                          "path"      : self.path}

        if sumisc.url_is_relative(url):
            url = sumisc.url_make_absolute(self.get_url(), url)

        return saga.filesystem.File(url=url, flags=flags, session=self.session,
                                     _adaptor=self._adaptor, _adaptor_state=adaptor_state)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def open_dir(self, url, flags):

        self._is_valid()

        adaptor_state = { "from_open" : True,
                          "url"       : saga.Url(self.url),   # deep copy
                          "path"      : self.path}

        if sumisc.url_is_relative(url):
            url = sumisc.url_make_absolute(self.get_url(), url)

        return saga.filesystem.Directory(url=url, flags=flags, session=self.session,
                _adaptor=self._adaptor, _adaptor_state=adaptor_state)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def change_dir(self, tgt, flags):

        tgt_url = saga.Url(tgt)

        # FIXME: attempt to get new EP
        if not sumisc.url_is_compatible(self.url, tgt_url):
            raise saga.BadParameter("Target dir outside of namespace '%s': %s" \
                                  % (self.url, tgt_url))

        if sumisc.url_is_relative(tgt_url):

            self.path      = tgt_url.path
            self.orig.path = self.path

        else:
            self.orig      = saga.Url(tgt_url)
            self.url       = tgt_url
            self.path      = self.url.path
            self.url.path  = None

        self.initialize()

        self._logger.debug("changed directory (%s)" % (tgt))

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def close(self, timeout=None):

        if timeout:
            raise saga.BadParameter("timeout for close not supported")

        self.finalize(kill=True)

    # ----------------------------------------------------------------
    @SYNC_CALL
    def get_url(self):

        self._is_valid()

        return saga.Url(self.orig) # deep copy

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self, npat, flags):

        self._is_valid()

        npat_ps  = self.get_path_spec(url=npat)
        out, err = self._adaptor.run_go_cmd(self.shell, "ls '%s'" % (npat_ps))
        lines = filter(None, out.split("\n"))
        self._logger.debug(lines)

        self.entries = []
        for line in lines:
            self.entries.append(saga.Url(line.strip()))

        return self.entries
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy_self(self, tgt, flags):

        self._is_valid()

        return self.copy(src_in=None, tgt_in=tgt, flags=flags)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy(self, src_in, tgt_in, flags, _from_task=None):

        self._is_valid()

        sync_level = 0
        
        src_ps = self.get_path_spec(url=src_in)
        tgt_ps = self.get_path_spec(url=tgt_in)

        cmd_flags = ""
        if flags & saga.filesystem.RECURSIVE:
            cmd_flags += "-r"

        if flags & saga.filesystem.CREATE_PARENTS:
            self._adaptor.mkparents(self.session, self.shell, tgt_ps)

        cmd      = "scp %s -s %d '%s' '%s'" % (cmd_flags, sync_level, src_ps, tgt_ps)
        out, err = self._adaptor.run_go_cmd(self.shell, cmd)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def move_self(self, tgt, flags):

        return self.move(src_in=None, tgt_in=tgt, flags=flags)
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def move(self, src_in, tgt_in, flags):

        # if src and target are on the same endpoint, we might get away with an
        # actual 'rename' command -- in all other cases (or if rename failed),
        # we fake move as non-atomic copy/remove...

        src_ps = self.get_path_spec(url=src_in)
        tgt_ps = self.get_path_spec(url=tgt_in)

        src_ep_str = src_ps.split(':', 1)[0]
        tgt_ep_str = tgt_ps.split(':', 1)[0]

        if src_ep_str == tgt_ep_str:

            try:
                self._adaptor.run_go_cmd(self.shell, "rename '%s' '%s'" % (src_ps, tgt_ps))
                return
            except:
                self._logger.warn("rename op failed -- retry as copy/remove")

        # either the op spans endpoints, or the 'rename' op failed
        self.copy(src_in, tgt_in, flags)
        self.remove(src_in, flags)
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove_self(self, flags):

        self._is_valid()

        self.remove(self.url, flags)
        self.invalid = True
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove(self, tgt_in, flags):

        self._is_valid()

        tgt_ps = self.get_path_spec(url=tgt_in)

        cmd_flags = ""
        if flags & saga.filesystem.RECURSIVE:
            cmd_flags += "-r"

        # oh this is just great... - a dir only gets removed (on some endpoints)
        # if the trailing '/' is specified -- otherwise the op *silently fails*!
        # Oh well, since we don't really (want to) know if the target is a dir
        # or not, we remove both versions... :/
        # FIXME
        cmd      = "rm %s -f '%s/'" % (cmd_flags, tgt_ps)
        out, err = self._adaptor.run_go_cmd(self.shell, cmd)

        cmd      = "rm %s -f '%s'"  % (cmd_flags, tgt_ps)
        out, err = self._adaptor.run_go_cmd(self.shell, cmd, mode='ignore')

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def make_dir(self, tgt_in, flags):

        self._is_valid()

        tgt_ps = self.get_path_spec(url=tgt_in)

        if flags & saga.filesystem.CREATE_PARENTS:
            self._adaptor.mkparents(self.session, self.shell, tgt_ps)

        else:
            cmd = "mkdir '%s'" % tgt_ps
            self._adaptor.run_go_cmd(self.shell, cmd)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_size_self(self):

        self._is_valid()

        return self.get_size(self.url)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_size(self, tgt_in):

        self._is_valid()

        tgt_ps = self.get_path_spec(url=tgt_in)
        cmd = "ls -al '%s/.'"  % tgt_ps
        out, _ = self._adaptor.run_go_cmd(self.shell, cmd)

        try:
            size = int(out.split('\n')[0].split()[3])
        except IndexError:
            msg = "split failed, out: %s" % out
            self._logger.error(msg)
            raise saga.NoSuccess(msg)

        return size

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_dir_self(self):

        self._is_valid()

        return self.is_dir(self.url)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_dir(self, tgt_in):

        self._is_valid()

        tgt_ps = self.get_path_spec(url=tgt_in)
        cmd = "ls -al '%s/.'"  % tgt_ps
        out, _ = self._adaptor.run_go_cmd(self.shell, cmd)

        field = out.split('\n')[0].split()[0][0]
        if field == 'd':
            return True
        elif field == '-':
            return False
        elif field == 'l':
            return False
        else:
            raise saga.NoSuccess("is_dir() unknown dirflag: '%s' (%s)" % (field, out))

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_link_self(self):

        self._is_valid()

        return self.is_link(self.url)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_link(self, tgt_in):

        self._is_valid()

        tgt_ps = self.get_path_spec(url=tgt_in)
        cmd = "ls -al '%s/.'"  % tgt_ps
        out, _ = self._adaptor.run_go_cmd(self.shell, cmd)

        field = out.split('\n')[0].split()[0][0]
        if field == 'd':
            return False
        elif field == '-':
            return False
        elif field == 'l':
            return True
        else:
            raise saga.NoSuccess("is_link() unknown field: '%s' (%s)" % (field, out))

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_file_self(self):

        self._is_valid()

        return self.is_link(self.url)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_file(self, tgt_in):

        self._is_valid()

        tgt_ps = self.get_path_spec(url=tgt_in)
        cmd = "ls -al '%s/.'"  % tgt_ps
        out, _ = self._adaptor.run_go_cmd(self.shell, cmd)

        field = out.split('\n')[0].split()[0][0]
        if field == 'd':
            return False
        elif field == '-':
            return True
        elif field == 'l':
            return False
        else:
            raise saga.NoSuccess("is_file() unknown field: '%s' (%s)" % (field, out))


###############################################################################
#
class GOFile(saga.adaptors.cpi.filesystem.File):
    """ Implements saga.adaptors.cpi.filesystem.File
    """
    # ----------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        _cpi_base = super(GOFile, self)
        _cpi_base.__init__(api, adaptor)

    # ----------------------------------------------------------------
    #
    def _is_valid(self):

        if not self.valid:
            raise saga.IncorrectState("this instance was closed or removed")

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, url, flags, session):
        """ File instance constructor """

        # FIXME: eval flags!
        if flags == None:
            flags = 0

        self.orig     = saga.Url(url) # deep copy
        self.url      = saga.Url(url) # deep copy
        self.path     = url.path       # keep path separate
        self.cwd      = sumisc.url_get_dirname(self.url)
        self.url.path = None

        self.flags    = flags
        self.session  = session
        self.valid    = False # will be set by initialize

        self.initialize()

        return self.get_api()

    # ----------------------------------------------------------------
    #
    def initialize(self):

        # GO shell got started, found its prompt.  Now, change
        # to the initial (or later current) working directory.

        self.shell = self._adaptor.get_go_shell(self.session)
        self.ep    = self._adaptor.get_go_endpoint(self.session, self.shell, self.url)
        self.ep_str, self.ep_name, self.ep_url = \
                     self._adaptor.get_go_endpoint_ids(self.session, self.url)
        ps         = self.get_path_spec()
        cwd_ps     = self.get_path_spec(path=self.cwd)

        if not self.ep:
            raise saga.BadParameter("invalid file '%s'" % ps)

        if self.flags & saga.filesystem.CREATE_PARENTS:
            self._adaptor.mkparents(self.session, self.shell, cwd_ps)

        elif self.flags & saga.filesystem.CREATE:
            self._logger.error("CREATE not supported for files via globus online")

        else:
            # this is as good an existence test as we can manage...
            self._adaptor.run_go_cmd(self.shell, "ls '%s'" % ps)

        self._logger.debug("Initialized file %s/%s" % (self.url, self.path))

        self.valid = True

    # ----------------------------------------------------------------
    #
    def get_path_spec(self, url=None, path=None):

        return self._adaptor.get_path_spec(
            session  = self.session,
            url      = url,
            path     = path,
            cwd_url  = self.url,
            cwd_path = self.path
        )

    # ----------------------------------------------------------------
    #
    def finalize(self, kill=False):

        if kill and self.shell:
            self.shell.finalize(True)
            self.shell = None

        self.valid = False

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def close(self, timeout=None):

        if timeout:
            raise saga.BadParameter("timeout for close not supported")

        self.finalize(kill=True)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url(self):

        self._is_valid()

        return saga.Url(self.orig) # deep copy

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy_self(self, tgt_in, flags):

        self._is_valid()

        # FIXME: eval flags
        sync_level = 0

        src_ps = self.get_path_spec()
        tgt_ps = self.get_path_spec(url=tgt_in)

        if flags & saga.filesystem.CREATE_PARENTS:
            self._adaptor.mkparents(self.session, self.shell, tgt_ps)

        cmd_flags = ""
        if flags & saga.filesystem.RECURSIVE:
            cmd_flags += "-r"

        cmd = "scp %s -s %d '%s' '%s'" % (cmd_flags, sync_level, src_ps, tgt_ps)
        out, err = self._adaptor.run_go_cmd(self.shell, cmd)
   
    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def move_self(self, tgt_in, flags):

        # if src and target are on the same endpoint, we might get away with an
        # actual 'rename' command -- in all other cases (or if rename failed),
        # we fake move as non-atomic copy/remove...

        src_ps = self.get_path_spec()
        tgt_ps = self.get_path_spec(url=tgt_in)

        src_ep_str = src_ps.split(':', 1)[0]
        tgt_ep_str = tgt_ps.split(':', 1)[0]

        if src_ep_str == tgt_ep_str:

            try:
                self._adaptor.run_go_cmd(self.shell, "rename '%s' '%s'" % (src_ps, tgt_ps))
                return
            except:
                self._logger.warn("Rename op failed -- retry as copy/remove")

        # either the op spans endpoints, or the 'rename' op failed
        self.copy_self(tgt_in, flags)
        self.remove_self(flags)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove_self(self, flags):

        self._is_valid()

        cmd_flags = ""
        if flags & saga.filesystem.RECURSIVE:
            cmd_flags += "-r"

        tgt_ps   = self.get_path_spec()
        cmd      = "rm %s -f '%s'"  % (cmd_flags, tgt_ps)
        out, err = self._adaptor.run_go_cmd(self.shell, cmd, mode='ignore')

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def get_size_self(self):

        self._is_valid()

        tgt_ps = self.get_path_spec(url=self.url)
        cmd = "ls -al '%s/.'"  % tgt_ps
        out, _ = self._adaptor.run_go_cmd(self.shell, cmd)

        try:
            size = int(out.split('\n')[0].split()[3])
        except IndexError:
            msg = "split failed, out: %s" % out
            self._logger.error(msg)
            raise saga.NoSuccess(msg)

        return size

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_link_self(self):
        self._is_valid()

        tgt_ps = self.get_path_spec(url=self.url)
        cmd = "ls -al '%s/.'"  % tgt_ps
        out, _ = self._adaptor.run_go_cmd(self.shell, cmd)

        field = out.split('\n')[0].split()[0][0]
        if field == 'd':
            return False
        elif field == '-':
            return False
        elif field == 'l':
            return True
        else:
            raise saga.NoSuccess("is_link() unknown field: '%s' (%s)" % (field, out))

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_file_self(self):
        self._is_valid()

        tgt_ps = self.get_path_spec(url=self.url)
        cmd = "ls -al '%s/.'"  % tgt_ps
        out, _ = self._adaptor.run_go_cmd(self.shell, cmd)

        field = out.split('\n')[0].split()[0][0]
        if field == 'd':
            return False
        elif field == '-':
            return True
        elif field == 'l':
            return False
        else:
            raise saga.NoSuccess("is_file() unknown field: '%s' (%s)" % (field, out))

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_dir_self(self):

        self._is_valid()

        tgt_ps = self.get_path_spec(url=self.url)
        cmd = "ls -al '%s/.'"  % tgt_ps
        out, _ = self._adaptor.run_go_cmd(self.shell, cmd)

        field = out.split('\n')[0].split()[0][0]
        if field == 'd':
            return True
        elif field == '-':
            return False
        elif field == 'l':
            return False
        else:
            raise saga.NoSuccess("is_dir() unknown field: '%s' (%s)" % (field, out))
