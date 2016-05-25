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


    def sanity_check(self):
        pass

    def file_get_size(self, shell, url):

        # In case of SURL the fields are:
        # file mode, number of links to the file, user id, group id, file size(bytes), locality, file name.
        # srm://srm.hep.fiu.edu:8443/srm/v2/server?SFN=/mnt/hadoop/osg/marksant/TESTFILE")
        # -rwxr-xr-x   1     1     2      19               ONLINE /mnt/hadoop/osg/marksant/TESTFILE
        rc, out, _ = shell.run_sync("lcg-ls -l -b -D srmv2 %s" % url)

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
        rc, out, _ = shell.run_sync("lcg-ls -d -l -b -D srmv2 %s" % url)

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            else:
                raise Exception("Couldn't list file")
        
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
        rc, out, _ = shell.run_sync("lcg-cp -v -b -D srmv2 %s %s" % (src, dst))

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

        rc, out, _ = shell.run_sync("lcg-del -l -b -D srmv2 %s" % tgt)

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

        if flags & saga.filesystem.RECURSIVE:
            rc, out, _ = shell.run_sync("srmrmdir --recursive %s" % tgt)
        else:
            rc, out, _ = shell.run_sync("lcg-del -d -l -b -D srmv2 %s" % tgt)

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            else:
                raise Exception("Remove failed.")


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
    @SYNC_CALL
    def init_instance(self, adaptor_state, url, flags, session):

        self._url       = saga.Url(url) # deep copy
        self._flags     = flags
        self._session   = session

        self._init_check()

        try:
            # open a shell
            self.shell = sups.PTYShell('fork://localhost/', self.session)
            # self.shell = sups.PTYShell('ssh://localhost/', self.session)

            # run grid-proxy-info, see if we get any errors -- if so, fail the
            # sanity check
            rc, out, _ = self.shell.run_sync("grid-proxy-info")
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

        rc, out, _ = self.shell.run_sync("lcg-ls -b -D srmv2 %s" % self._url)

        if rc != 0:
            if 'SRM_INVALID_PATH' in out:
                raise saga.exceptions.DoesNotExist(url)
            else:
                raise Exception("Couldn't list directory.")

        return [x.rsplit('/', 1)[1] for x in out.split()]


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def make_dir(self, tgt_in, flags):
        url = self._adaptor.surl2query(self._url, self._surl, tgt_in)

        rc, out, _ = self.shell.run_sync("srmmkdir %s" % url)

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
    def remove(self, flags, tgt):
        self._adaptor.srm_dir_remove(self.shell, flags, tgt)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove_self(self, flags):
        self.remove(flags, self._url)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy_self(self, tgt, flags):
        return self.copy(src_in=None, tgt_in=tgt, flags=flags)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy(self, src, tgt, flags):

        self._adaptor.srm_transfer(self.shell, flags, src, tgt)

    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def exists(self, tgt):

        try:
            self._adaptor.srm_stat(self.shell, tgt)
        except saga.DoesNotExist:
            return False

        return True


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


    @SYNC_CALL
    def init_instance(self, adaptor_state, url, flags, session):

        self._url     = url
        self._flags   = flags
        self._session = session

        self._init_check()

        try:
            # open a shell
            self.shell = sups.PTYShell('fork://localhost/', self.session)
            # self.shell = sups.PTYShell('ssh://localhost/', self.session)

            # run grid-proxy-info, see if we get any errors -- if so, fail the
            # sanity check
            rc, _, _ = self.shell.run_sync("grid-proxy-info")
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
        return self._adaptor.file_get_size(self.shell, self._url)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def copy_self(self, dst, flags):

        self._adaptor.srm_transfer(self.shell, flags, self._url, dst)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def remove_self(self, flags):

        self._adaptor.srm_file_remove(self.shell, flags, self._url)


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_file_self(self):
        stat = self._adaptor.srm_stat(self.shell, self._url)

        if stat['mode'] == 'file':
            return True
        else:
            return False


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_link_self(self):
        stat = self._adaptor.srm_stat(self.shell, self._url)

        if stat['mode'] == 'link':
            return True
        else:
            return False


    # ----------------------------------------------------------------
    #
    @SYNC_CALL
    def is_dir_self(self):
        stat = self._adaptor.srm_stat(self.shell, self._url)

        if stat['mode'] == 'dir':
            return True
        else:
            return False


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
