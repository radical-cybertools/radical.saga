
import saga.adaptors.cpi.decorators as CPI
import saga.adaptors.cpi.namespace  as ns_cpi


# keep order of inheritance!  super() below uses MRO
class Directory (ns_cpi.directory.Directory) :

    # ----------------------------------------------------------------
    #
    # initialization methods
    #
    def __init__ (self, api, adaptor) :

        self._cpi_nsdirec = super  (Directory, self)
        self._cpi_nsdirec.__init__ (api, adaptor)

    @CPI.SYNC
    def init_instance         (self, url, flags, session) : pass
    @CPI.ASYNC
    def init_instance_async   (self, url, flags, session) : pass


    # ----------------------------------------------------------------
    #
    # add filesystem directory methods
    #
    @CPI.SYNC
    def get_size              (self, name, flags, ttype)  : pass
    @CPI.ASYNC
    def get_size_async        (self, name, flags, ttype)  : pass

    @CPI.SYNC
    def get_size_self         (self,       flags, ttype)  : pass
    @CPI.ASYNC                                
    def get_size__self_async  (self,       flags, ttype)  : pass

    @CPI.SYNC
    def is_file               (self, name,        ttype)  : pass
    @CPI.ASYNC
    def is_file_async         (self, name,        ttype)  : pass

    @CPI.SYNC
    def is_file_self          (self,              ttype)  : pass
    @CPI.ASYNC                                        
    def is_file_self_async    (self,              ttype)  : pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

