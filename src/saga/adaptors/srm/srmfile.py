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
_ADAPTOR_OPTIONS       = []
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
    'version'          : 'v0.2',
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

CONNTIMEOUT = 60
SNDTIMEOUT = 7200
SRMTIMEOUT = 180

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

        self.cfg = self.get_config('saga.adaptor.srm_file')
        if 'pty_url' in self.cfg:
            self.pty_url = self.cfg['pty_url'].get_value()
        else:
            self.pty_url = 'fork://localhost/'


    def sanity_check(self):
        pass


    def file_get_size(self, shell, url):

        # In case of SURL the fields are:
        # file mode, number of links to the file, user id, group id, file size(bytes), locality, file name.
        # srm://srm.hep.fiu.edu:8443/srm/v2/server?SFN=/mnt/hadoop/osg/marksant/TESTFILE")
        # -rwxr-xr-x   1     1     2      19               ONLINE /mnt/hadoop/osg/marksant/TESTFILE
        try:
            # 'lcg-ls' uses the wrong timeout setting for connection timeout
            rc, out, _ = shell.run_sync("lcg-ls --sendreceive-timeout %d -l -b -D srmv2 %s" % (CONNTIMEOUT, url))
        except:
            shell.finalize(kill_pty=True)
            raise Exception("get_size failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            else:
                raise Exception("Couldn't list file")

        # Sometimes we get cksum too, which we ignore
        fields = out.split()[:7]
        _, _, _, _, size_str, _, _ = fields

        size = int(size_str)

        return size


    def srm_stat(self, shell, url):

        # In case of SURL the fields are:
        # file mode, number of links to the file, user id, group id, file size(bytes), locality, file name.
        # srm://srm.hep.fiu.edu:8443/srm/v2/server?SFN=/mnt/hadoop/osg/marksant/TESTFILE")
        # -rwxr-xr-x   1     1     2      19               ONLINE /mnt/hadoop/osg/marksant/TESTFILE
        try:
            # 'lcg-ls' uses the wrong timeout setting for connection timeout
            rc, out, _ = shell.run_sync("lcg-ls --sendreceive-timeout %d -d -l -b -D srmv2 %s" % (CONNTIMEOUT, url))
        except:
            shell.finalize(kill_pty=True)
            raise Exception("stat failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            elif not out:
                raise saga.exceptions.Timeout("Connection timeout")
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
            rc, out, _ = shell.run_sync('lcg-cp -v -b -D srmv2 '
                '--sendreceive-timeout %d --connect-timeout %d '
                '--srm-timeout %d %s %s' % (
                SNDTIMEOUT, CONNTIMEOUT, SRMTIMEOUT, src, dst))
        except:
            shell.finalize(kill_pty=True)
            raise Exception("transfer failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            else:
                raise Exception("Copy failed.")


    # --------------------------------------------------------------------------
    #
    def srm_file_remove(self, shell, flags, tgt):

        if isinstance(tgt, saga.filesystem.file.File):
            tgt = tgt.get_url()

        try:
            rc, out, _ = shell.run_sync("lcg-del -l -b -D srmv2 %s" % tgt)
        except:
            shell.finalize(kill_pty=True)
            raise Exception("remove failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
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

        files, dirs = self.srm_list_kind(shell, tgt)
        for d in dirs:
            url = tgt + '/' + d
            self.srm_dir_remove(shell, flags, url)

        for f in files:
            url = tgt + '/' + f
            self.srm_file_remove(shell, flags, url)

        # Finally remove self
        try:
            rc, out, _ = shell.run_sync("lcg-del -d -l -b -D srmv2 %s" % tgt)
        except:
            shell.finalize(kill_pty=True)
            raise Exception("remove failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
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
            # 'lcg-ls' uses the wrong timeout setting for connection timeout
            rc, out, _ = shell.run_sync("lcg-ls --sendreceive-timeout %d -b -D srmv2 %s" % (CONNTIMEOUT, url))
        except:
            shell.finalize(kill_pty=True)
            raise Exception("list failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            else:
                raise Exception("Couldn't list directory.")

        return [x.rsplit('/', 1)[1] for x in out.split()]


    # --------------------------------------------------------------------------
    #
    def srm_list_kind(self, shell, url):

        try:
            # 'lcg-ls' uses the wrong timeout setting for connection timeout
            rc, out, _ = shell.run_sync("lcg-ls --sendreceive-timeout %d -l -b -D srmv2 %s" % (CONNTIMEOUT, url))
        except:
            shell.finalize(kill_pty=True)
            raise Exception("list failed")

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            else:
                raise Exception("Couldn't list directory.")

        # Drop checksum entries
        # * Checksum: 8ea8c8a7 (ADLER32)
        entries = [x for x in out.split('\n') if not '* Checksum:' in x]

        files = []
        dirs = []
        # Output format
        #d--------- 1 0 0 0 UNKNOWN /xrd/vos/osg/marksant/data/tmp
        #---------- 1 0 0 1048576 UNKNOWN /xrd/vos/osg/marksant/data/1M
        for entry in entries:
            if not entry:
                continue

            kind = entry.split()[0][0]
            name = entry.rsplit('/', 1)[1]
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

            # run grid-proxy-info, see if we get any errors -- if so, fail the
            # sanity check
            try:
                rc, out, _ = self.shell.run_sync("grid-proxy-info")
            except:
                shell.finalize(kill_pty=True)
                raise Exception("grid-proxy-info failed")

            if rc != 0:
                raise Exception("grid-proxy-info failed")
            
            if 'timeleft : 0:00:00' in out:
                raise Exception("x509 proxy expired.")

        except:
            raise saga.NoSuccess("Check environment and certificates.")

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
            shell.finalize(kill_pty=True)
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

            # run grid-proxy-info, see if we get any errors -- if so, fail the
            # sanity check
            try:
                rc, _, _ = self.shell.run_sync("grid-proxy-info")
            except:
                shell.finalize(kill_pty=True)
                raise Exception("grid-proxy-info failed")

            if rc != 0:
                raise Exception("grid-proxy-info failed")

            if 'timeleft : 0:00:00' in out:
                raise Exception("x509 proxy expired.")

        except:
            raise saga.NoSuccess("Check environment and certificates.")

        return self.get_api()


    def _init_check(self):

        url   = self._url
        flags = self._flags 

        # if url.query :
        #     raise saga.exceptions.BadParameter ("Cannot handle url %s (has query)"     %  url)
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
