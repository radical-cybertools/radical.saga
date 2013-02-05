""" Local filesystem adaptor implementation """

import os
import pprint
import shutil
import traceback
import stat
import errno

import saga.url
import saga.adaptors.cpi.base
import saga.adaptors.cpi.filesystem
import saga.utils.misc

# Grid File Access Library
import gfal2

SYNC_CALL  = saga.adaptors.cpi.base.SYNC_CALL


###############################################################################
# adaptor info
#

_ADAPTOR_NAME          = 'saga.adaptor.filesystem.srm'
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
    'version'          : 'v0.1',
    'schemas'          : _ADAPTOR_SCHEMAS,
    'cpis'             : [{
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

class Adaptor (saga.adaptors.cpi.base.AdaptorBase):
    """ 
    This is the actual adaptor class, which gets loaded by SAGA (i.e. by the
    SAGA engine), and which registers the CPI implementation classes which
    provide the adaptor's functionality.
    """

    def __init__ (self) :

        saga.adaptors.cpi.base.AdaptorBase.__init__ (self, _ADAPTOR_INFO, _ADAPTOR_OPTIONS)


    def sanity_check (self) :
        # TODO
        pass



###############################################################################
#
class SRMDirectory (saga.adaptors.cpi.filesystem.Directory, saga.adaptors.cpi.Async) :

    def __init__ (self, api, adaptor) :

        saga.adaptors.cpi.CPIBase.__init__ (self, api, adaptor)



    @SYNC_CALL
    def init_instance (self, adaptor_state, url, flags, session) :

        self._url       = url
        self._flags     = flags
        self._session   = session

        self.gfal_context = gfal2.creat_context()

        self._init_check ()

        return self.get_api ()


    def _init_check (self) :

        url   = self._url
        flags = self._flags 

        if url.fragment :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has fragment)"     %  url)
        if url.query :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has query)"     %  url)
        if url.username :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has username)"  %  url)
        if url.password :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has password)"  %  url)

        self._path = url.path
        path       = url.path

        try:
            self.st = self.gfal_context.stat(str(url))
        except gfal2.GError, e:
            code = e.code()
            if code == errno.ENOENT:
                raise saga.exceptions.DoesNotExist(str(url))
                
        if not stat.S_ISDIR(self.st.st_mode):
            raise saga.exceptions.NoSuccess("URL is not a directory:" + str(url))


    @SYNC_CALL
    def get_url (self) :

        return self._url


    @SYNC_CALL
    def copy (self, source, target, flags) :


        src_url = saga.url.Url (source)
        src     = src_url.path
        tgt_url = saga.url.Url (target)
        tgt     = tgt_url.path


        if src_url.schema :
            if not src_url.schema.lower () in _ADAPTOR_SCHEMAS :
                raise saga.exceptions.BadParameter ("Cannot handle url %s (not srm)" %  source)

        if src[0] != '/' :
            src = "%s/%s"   % (os.path.dirname (src), src)


        if tgt_url.schema :
            if not tgt_url.schema.lower () in _ADAPTOR_SCHEMAS :
                raise saga.exceptions.BadParameter ("Cannot handle url %s (not srm)" %  target)

        if tgt[0] != '/' :
            tgt = "%s/%s"   % (os.path.dirname (src), tgt)

        print "sync copy %s -> %s" % (src, tgt)
        shutil.copy2 (src, tgt)





######################################################################
#
# file adaptor class
#
class SRMFile(saga.adaptors.cpi.filesystem.File):

    def __init__(self, api, adaptor):
        saga.adaptors.cpi.CPIBase.__init__(self, api, adaptor)



    def _dump(self):
        print "url    : %s"  % self._url
        print "flags  : %s"  % self._flags
        print "session: %s"  % self._session


    @SYNC_CALL
    def init_instance(self, adaptor_state, url, flags, session):

        self._url     = url
        self._flags   = flags
        self._session = session

        self.gfal_context = gfal2.creat_context()

        self._init_check ()

        return self



    def _init_check (self) :

        url   = self._url
        flags = self._flags 

        if url.query :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has query)"     %  url)
        if url.username :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has username)"  %  url)
        if url.password :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (has password)"  %  url)

        self._path = url.path
        path       = url.path

        try:
            self.st = self.gfal_context.stat(str(url))
        except gfal2.GError, e:
            code = e.code()
            if code == errno.ENOENT:
                raise saga.exceptions.DoesNotExist(str(url))
                
        if not stat.S_ISREG(self.st.st_mode):
            raise saga.exceptions.NoSuccess("URL is not a file:)" + str(url))


        #if not os.path.exists (dirname) :
        #        if saga.filesystem.CREATE_PARENTS & flags :
        #            try :
        #                os.makedirs (path)
        #            except Exception as e :
        #                raise saga.exceptions.NoSuccess ("Could not 'mkdir -p %s': %s)"  \
        #                                                % (path, str(e)))
        #        else :
        #            raise saga.exceptions.BadParameter ("Cannot handle url %s (parent dir does not exist)"  \
        #                                             %  path)
        
        #if not os.path.exists (filename) :
        #        if saga.filesystem.CREATE & flags :
        #            try :
        #                open (path, 'w').close () # touch
        #            except Exception as e :
        #                raise saga.exceptions.NoSuccess ("Could not 'touch %s': %s)"  \
        #                                                % (path, str(e)))
        #        else :
        #            raise saga.exceptions.BadParameter ("Cannot handle url %s (file does not exist)"  \
        #                                             %  path)
        
        #if not os.path.isfile (path) :
        #    raise saga.exceptions.BadParameter ("Cannot handle url %s (is not a file)"  \
        #                                       %  path)

    @SYNC_CALL
    def get_url (self) :
        return self._url


    @SYNC_CALL
    def get_size_self (self) :
        return self.st.st_size


    @SYNC_CALL
    def copy_self(self, target, flags):

        tgt_url = saga.url.Url (target)
        tgt     = tgt_url.path
        src     = self._url.path

        if tgt_url.schema :
            if not tgt_url.schema.lower () in _ADAPTOR_SCHEMAS :
                raise saga.exceptions.BadParameter ("Cannot handle url %s (not local)" %  target)

        if not saga.utils.misc.url_is_local (tgt_url) :
            raise saga.exceptions.BadParameter ("Cannot handle url %s (not local)"     %  target)

        if tgt[0] != '/' :
            tgt = "%s/%s"   % (os.path.dirname (src), tgt)

        print " copy %s %s" % (self._url, tgt)
        shutil.copy2 (src, tgt)




# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
