
from   saga.adaptors.cpi.base      import CPI_SYNC_CALL  as SYNC
from   saga.adaptors.cpi.base      import CPI_ASYNC_CALL as ASYNC

import saga.adaptors.cpi.namespace.directory


class Directory (saga.adaptors.cpi.namespace.directory.Directory) :

    # ----------------------------------------------------------------
    #
    # add filesystem directory methods
    #
    @SYNC
    def get_size              (self, name, flags, ttype)  : pass
    @ASYNC
    def get_size_async        (self, name, flags, ttype)  : pass

    @SYNC
    def get_size_self         (self,       flags, ttype)  : pass
    @ASYNC                                
    def get_size__self_async  (self,       flags, ttype)  : pass

    @SYNC
    def is_file               (self, name,        ttype)  : pass
    @ASYNC
    def is_file_async         (self, name,        ttype)  : pass

    @SYNC
    def is_file_self          (self,              ttype)  : pass
    @ASYNC                                        
    def is_file_self_async    (self,              ttype)  : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

