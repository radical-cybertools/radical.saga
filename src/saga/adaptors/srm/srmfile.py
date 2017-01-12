""" Local filesystem adaptor implementation """

import os
import pprint
import shutil
import traceback
import stat
import errno

import saga.url
import saga.adaptors.base
import saga.adaptors.cpi.filesystem
import saga.utils.misc
import saga.utils.pty_shell as sups

from saga.adaptors.cpi.decorators import SYNC_CALL


###############################################################################
# adaptor info
#

_ADAPTOR_NAME          = 'saga.adaptor.srm_file'
_ADAPTOR_SCHEMAS       = ['srm']
_ADAPTOR_OPTIONS       = [
    {
        'category': 'saga.adaptor.srm_file',
        'name': 'pty_url',
        'type': str,
        'default': 'fork://localhost/',
        'documentation': '''The local or remote url the adaptor connects to.''',
        'env_variable': None
    }
]
_ADAPTOR_CAPABILITIES  = {}

_ADAPTOR_DOC           = {
    'name'             : _ADAPTOR_NAME,
    'cfg_options'      : _ADAPTOR_OPTIONS,
    'capabilities'     : _ADAPTOR_CAPABILITIES,
    'description'      : 'The SRM filesystem adaptor.',
    'details'          : """This adaptor interacts with SRM Storage Elements
                         """,
    'schemas'          : {'srm': 'srm filesystem.'}

}

_ADAPTOR_INFO          = {
    'name'             : _ADAPTOR_NAME,
    'version'          : 'v0.3',
    'schemas'          : _ADAPTOR_SCHEMAS,
    'cpis'             : [
        {
        'type'         : 'saga.namespace.Directory',
        'class'        : 'SRMDirectory'
        },
        {
        'type'         : 'saga.namespace.Entry',
        'class'        : 'SRMFile'
        },
        {
        'type'         : 'saga.filesystem.Directory',
        'class'        : 'SRMDirectory'
        },
        {
        'type'         : 'saga.filesystem.File',
        'class'        : 'SRMFile'
        }
    ]
}

TRANSFER_TIMEOUT = 3600 # Timeout of the SRM plugin for the transfer
OPERATION_TIMEOUT = 3600 # Should be greater or equal than TRANSFER_TIMEOUT
CONNECTION_TIMEOUT = 180 # Technically same as OPERATION_TIMEOUT,
                         # but used for non-transfer operations.

###############################################################################
# The adaptor class

class Adaptor(saga.adaptors.base.Base):
    """
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """

    def __init__(self) :
        saga.adaptors.base.Base.__init__(self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)

        self.cfg = self.get_config(_ADAPTOR_NAME)
        self.pty_url = self.cfg['pty_url'].get_value()


    def sanity_check(self):
        pass


    def file_get_size(self, shell, url):

        try:
            # Following columns are displayed for each entry:
            # mode, number of links, group id, userid, size, last modification time, and name.
            rc, out, _ = shell.run_sync("gfal-ls --color never --timeout %d --long %s" % (CONNECTION_TIMEOUT, url))
        except:
            shell.finalize(kill_pty=True)
            raise Exception("get_size failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            else:
                raise Exception("Couldn't list file")

        fields = out.split()
        # -rw-r--r-- 1 45 44 19 May 30 15:29 srm://osg-se.sprace.org.br:8443/srm/managerv2?SFN=/pnfs/sprace.org.br/data/osg/marksant/TESTFILE
        _, _, _, _, size_str, _, _, _, _ = fields

        return int(size_str)


    def srm_stat(self, shell, url):

        # In case of SURL the fields are:
        # file mode, number of links to the file, user id, group id, file size(bytes), locality, file name.
        # srm://srm.hep.fiu.edu:8443/srm/v2/server?SFN=/mnt/hadoop/osg/marksant/TESTFILE")
        # -rwxr-xr-x   1     1     2      19               ONLINE /mnt/hadoop/osg/marksant/TESTFILE
        try:
            # Following columns are displayed for each entry:
            # mode, number of links, group id, userid, size, last modification time, and name.
            rc, out, _ = shell.run_sync(
                "gfal-ls --color never --timeout %d --directory --long %s" % (CONNECTION_TIMEOUT, url))
        except:
            shell.finalize(kill_pty=True)
            raise Exception("stat failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            if 'SRM_FAILURE' in out and 'forbidden' in out:
                raise saga.exceptions.AuthorizationFailed(url)
            if 'Command timed out after' in out:
                raise saga.exceptions.Timeout("Connection timeout")
            if 'Communication error on send' in out:
                # (gfal-ls error: 70 (Communication error on send) -
                # srm-ifce err: Communication error on send,
                # err: [SE][Ls][] httpg://cit-se.ultralight.org:8443/srm/v2/server:
                # CGSI-gSOAP running on nodo86 reports could not open connection
                # to cit-se.ultralight.org:8443\n\n\n)
                raise saga.exceptions.NoSuccess("Connection failed")
            else:
                raise saga.exceptions.NoSuccess("Couldn't list file")

        # Sometimes we get cksum too, which we ignore
        fields = out.split()[:7]
        stat_str, _, _, _, size_str, _, _ = fields

        mode = stat_str[0]
        if mode == '-':
            mode = 'file'
        elif mode == 'd':
            mode = 'dir'
        elif mode == 'l':
            mode = 'link'
        else:
            raise saga.NoSuccess("stat() unknown mode: '%s' (%s)" % (mode, out))

        size = int(size_str)

        return {
            'mode': mode,
            'size': size
        }


    # --------------------------------------------------------------------------
    #
    def srm_transfer(self, shell, flags, src, dst):

        if isinstance(src, saga.Url):
            src = src.__str__()
        if isinstance(dst, saga.filesystem.file.File):
            dst = dst.get_url()
        try:
            rc, out, _ = shell.run_sync('gfal-copy --parent --timeout %d --transfer-timeout %d %s %s' % (
                OPERATION_TIMEOUT, TRANSFER_TIMEOUT, src, dst))
        except:
            shell.finalize(kill_pty=True)
            raise Exception("transfer failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(src)
            elif '(File exists)' in out:
                raise saga.exceptions.AlreadyExists(dst)
            elif 'Could not open destination' in out:
                raise saga.exceptions.DoesNotExist(dst)
            else:
                raise Exception("Copy failed.")


    # --------------------------------------------------------------------------
    #
    def srm_file_remove(self, shell, flags, tgt):

        if isinstance(tgt, saga.filesystem.file.File):
            tgt = tgt.get_url()

        try:
            rc, out, _ = shell.run_sync("gfal-rm --timeout %d %s" % (CONNECTION_TIMEOUT, tgt))
        except:
            shell.finalize(kill_pty=True)
            raise Exception("remove failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(tgt)
            else:
                raise Exception("Remove failed.")


    # --------------------------------------------------------------------------
    #
    def srm_dir_remove(self, shell, flags, tgt):

        if isinstance(tgt, saga.filesystem.directory.Directory):
            tgt = tgt.get_url()
        if isinstance(tgt, saga.filesystem.file.File):
            tgt = tgt.get_url()
        if isinstance(tgt, saga.Url):
            tgt = str(tgt)

        try:
            rc, out, _ = shell.run_sync("gfal-rm --recursive %s" % tgt)
        except:
            shell.finalize(kill_pty=True)
            raise Exception("remove failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(tgt)
            else:
                raise Exception("Remove failed.")

    # --------------------------------------------------------------------------
    #
    def srm_list(self, shell, url, npat, flags):

        if npat:
            raise saga.exceptions.NotImplemented("no pattern selection")

        if isinstance(url, saga.filesystem.directory.Directory):
            url = url.get_url()
        if isinstance(url, saga.filesystem.file.File):
            url = url.get_url()

        try:
            rc, out, _ = shell.run_sync("gfal-ls --color never --timeout %d %s" % (CONNECTION_TIMEOUT, url))
        except:
            shell.finalize(kill_pty=True)
            raise Exception("list failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            else:
                raise Exception("Couldn't list directory.")

        return out.split('\n')


    # --------------------------------------------------------------------------
    #
    def srm_list_kind(self, shell, url):

        try:
            rc, out, _ = shell.run_sync("gfal-ls --color never --timeout %d --long %s" % (CONNECTION_TIMEOUT, url))
        except:
            shell.finalize(kill_pty=True)
            # TODO: raise something else or catch better?
            raise Exception("list failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            else:
                raise Exception("Couldn't list directory.")

        entries = out.split('\n')

        files = []
        dirs = []
        # Output format
        # ----------   1 0     0     1048576000 May 24 23:14 1000M
        # ----------   1 0     0     104857600 May 24 23:13 100M
        # ----------   1 0     0      10485760 May 24 23:13 10M
        # ----------   1 0     0       1048576 May 24 22:59 1M
        # d---------   1 0     0             0 Jun  1 11:37 tmp
        for entry in entries:
            if not entry:
                continue

            kind = entry[0]
            name = entry.split()[8]
            if kind == '-':
                files.append(name)
            elif kind == 'd':
                dirs.append(name)

        return (files, dirs)


    # --------------------------------------------------------------------------
    #
    def surl2query(self, url, surl, tgt_in):
        url = saga.Url(url)
        if tgt_in:
            surl = os.path.join(surl, str(tgt_in))
        url.query = 'SFN=%s' % surl
        return url


###############################################################################
#
class SRMDirectory (saga.adaptors.cpi.filesystem.Directory):

    # --------------------------------------------------------------------------
    #
    def __init__(self, api, adaptor):

        _cpi_base = super(SRMDirectory, self)
        _cpi_base.__init__(api, adaptor)


    # --------------------------------------------------------------------------
    #
    def _alive(self):
        alive = self.shell.alive()
        if not alive:
            self.shell = sups.PTYShell(self._adaptor.pty_url)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def init_instance(self, adaptor_state, url, flags, session):

        self._url       = saga.Url(url) # deep copy
        self._flags     = flags
        self._session   = session

        self._init_check()

        try:
            # open a shell
            self.shell = sups.PTYShell(self._adaptor.pty_url, self.session)
        except:
            raise saga.NoSuccess("Couldn't open shell (%s)" % self._adaptor.pty_url)

        #
        # Test for valid proxy
        #
        try:
            rc, out, _ = self.shell.run_sync("grid-proxy-info")
        except:
            self.shell.finalize(kill_pty=True)
            raise saga.exceptions.NoSuccess("grid-proxy-info failed (runsync)")

        if rc != 0:
            raise saga.exceptions.NoSuccess("grid-proxy-info failed (rc!=0)")

        if 'timeleft : 0:00:00' in out:
            raise saga.exceptions.AuthenticationFailed("x509 proxy expired.")

        #
        # Test for gfal2 tool
        #
        try:
            rc, _, _ = self.shell.run_sync("gfal2_version")
        except:
            self.shell.finalize(kill_pty=True)
            raise saga.exceptions.NoSuccess("gfal2_version")

        if rc != 0:
            raise saga.exceptions.DoesNotExist("gfal2 client not found")

        return self.get_api()


    # --------------------------------------------------------------------------
    #
    def _init_check(self):

        url   = self._url
        flags = self._flags

        if url.fragment :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has fragment)"  %  url)
        if url.username :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has username)"  %  url)
        if url.password :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has password)"  %  url)

        self._path = url.path
        (prefix, surl) = url.query.split('=')
        if prefix != 'SFN':
            raise saga.exceptions.BadParameter("SURL prefix %s is not SFN." % prefix)
        self._surl = surl


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_url(self):
        return self._url


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def list(self, npat, flags):

        self._alive()

        url = self._adaptor.surl2query(self._url, self._surl, None)
        return self._adaptor.srm_list(self.shell, url, npat, flags)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def make_dir(self, tgt_in, flags):

        self._alive()

        url = self._adaptor.surl2query(self._url, self._surl, tgt_in)

        try:
            rc, out, _ = self.shell.run_sync("srmmkdir %s" % url)
        except:
            self.shell.finalize(kill_pty=True)
            raise Exception(" failed")

        if rc != 0:
            if 'SRM_DUPLICATION_ERROR' in out:
                # Throw exception only if Exclusive flag was set.
                if flags & saga.filesystem.EXCLUSIVE:
                    raise saga.exceptions.AlreadyExists(url)
            else:
                raise Exception("Couldn't create directory.")


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_dir_self(self):
        return self.is_dir(None)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def is_dir(self, tgt_in):
        url = self._adaptor.surl2query(self._url, self._surl, tgt_in)
        stat = self._adaptor.srm_stat(self.shell, url)

        if stat['mode'] == 'dir':
            return True
        else:
            return False


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_link_self(self):
        return self.is_link(None)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def is_link(self, tgt_in):
        url = self._adaptor.surl2query(self._url, self._surl, tgt_in)
        stat = self._adaptor.srm_stat(self.shell, url)

        if stat['mode'] == 'link':
            return True
        else:
            return False


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_file_self(self):
        return self.is_file(None)


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def is_file(self, tgt_in):
        url = self._adaptor.surl2query(self._url, self._surl, tgt_in)
        stat = self._adaptor.srm_stat(self.shell, url)

        if stat['mode'] == 'file':
            return True
        else:
            return False


    # --------------------------------------------------------------------------
    #
    @SYNC_CALL
    def get_size(self, tgt_in):
        if '/' in tgt_in:
            # Assume absolute URI
            url = tgt_in
        else:
            url = self._adaptor.surl2query(self._url, self._surl, tgt_in)
        return self._adaptor.file_get_size(self.shell, url)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove(self, tgt, flags):

        if flags & saga.filesystem.RECURSIVE:
            self._adaptor.srm_dir_remove(self.shell, flags, tgt)
        else:
            self._adaptor.srm_file_remove(self.shell, flags, tgt)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove_self(self, flags):
        self.remove(self._url, flags)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy_self(self, tgt, flags):

        return self.copy(src_in=None, tgt_in=tgt, flags=flags)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy(self, src, tgt, flags):
        self._alive()

        self._adaptor.srm_transfer(self.shell, flags, src, tgt)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def exists(self, tgt):
        self._alive()

        try:
            self._adaptor.srm_stat(self.shell, tgt)
        except saga.exceptions.DoesNotExist:
            return False

        return True

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def close(self, timeout=None):

        if timeout:
            raise saga.BadParameter("timeout for close not supported")


######################################################################
#
# file adaptor class
#
class SRMFile(saga.adaptors.cpi.filesystem.File):

    def __init__(self, api, adaptor):
        _cpi_base = super(SRMFile, self)
        _cpi_base.__init__(api, adaptor)


    def _dump(self):
        print "url    : %s"  % self._url
        print "flags  : %s"  % self._flags
        print "session: %s"  % self._session


    # --------------------------------------------------------------------------
    #
    def _alive(self):
        alive = self.shell.alive()
        if not alive:
            self.shell = sups.PTYShell(self._adaptor.pty_url)


    @SYNC_CALL
    def init_instance(self, adaptor_state, url, flags, session):

        self._url     = url
        self._flags   = flags
        self._session = session

        self._init_check()

        try:
            # open a shell
            self.shell = sups.PTYShell(self._adaptor.pty_url, self.session)
        except:
            raise saga.NoSuccess("Couldn't open shell")

        #
        # Test for valid proxy
        #
        try:
            rc, out, _ = self.shell.run_sync("grid-proxy-info")
        except:
            self.shell.finalize(kill_pty=True)
            raise saga.exceptions.NoSuccess("grid-proxy-info failed")

        if rc != 0:
            raise saga.exceptions.NoSuccess("grid-proxy-info failed")

        if 'timeleft : 0:00:00' in out:
            raise saga.exceptions.AuthenticationFailed("x509 proxy expired.")

        #
        # Test for gfal2 tool
        #
        try:
            rc, _, _ = self.shell.run_sync("gfal2_version")
        except:
            self.shell.finalize(kill_pty=True)
            raise saga.exceptions.NoSuccess("gfal2_version")

        if rc != 0:
            raise saga.exceptions.DoesNotExist("gfal2 client not found")

        return self.get_api()


    def _init_check(self):

        url   = self._url
        flags = self._flags 

        if url.username :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has username)"  %  url)
        if url.password :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has password)"  %  url)

        self._path = url.path
        path       = url.path


    @SYNC_CALL
    def get_url(self):
        return self._url


    @SYNC_CALL
    def get_size_self(self):
        self._alive()
        return self._adaptor.file_get_size(self.shell, self._url)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy_self(self, dst, flags):
        self._alive()

        self._adaptor.srm_transfer(self.shell, flags, self._url, dst)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove_self(self, flags):
        self._alive()

        self._adaptor.srm_file_remove(self.shell, flags, self._url)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_file_self(self):
        self._alive()
        stat = self._adaptor.srm_stat(self.shell, self._url)

        if stat['mode'] == 'file':
            return True
        else:
            return False


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_link_self(self):
        self._alive()
        stat = self._adaptor.srm_stat(self.shell, self._url)

        if stat['mode'] == 'link':
            return True
        else:
            return False


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_dir_self(self):
        self._alive()
        stat = self._adaptor.srm_stat(self.shell, self._url)

        if stat['mode'] == 'dir':
            return True
        else:
            return False


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def close(self, timeout=None):

        if timeout:
            raise saga.BadParameter("timeout for close not supported")


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
