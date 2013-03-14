
""" Provides the SAGA Resource CPI """

import saga.adaptors.cpi.decorators as cpi_dec
import saga.adaptors.cpi.base       as cpi_base
import saga.adaptors.cpi.async      as cpi_async

SYNC  = cpi_dec.CPI_SYNC_CALL
ASYNC = cpi_dec.CPI_ASYNC_CALL


class Manager (cpi_base.CPIBase, cpi_async.Async) :

    # ----------------------------------------------------------------
    #
    # initialization methods
    #
    def __init__ (self, api, adaptor) :

        self._cpi_base = super  (Service, self)
        self._cpi_base.__init__ (api, adaptor)

    @SYNC
    def init_instance              (self, url, session)        : pass
    @ASYNC
    def init_instance_async        (self, url, session)        : pass



    # ----------------------------------------------------------------
    #
    # resource manager methods
    #
    @SYNC
    def list                       (self, type,      ttype)    : pass
    @ASYNC
    def list                       (self, type,      ttype)    : pass

    @SYNC
    def get_description            (self, id,        ttype)    : pass
    @ASYNC
    def get_description            (self, id,        ttype)    : pass

    @SYNC
    def list_templates             (self, type,      ttype)    : pass
    @ASYNC
    def list_templates             (self, type,      ttype)    : pass

    @SYNC
    def get_template               (self, name,      ttype)    : pass
    @ASYNC
    def get_template               (self, name,      ttype)    : pass

    @SYNC
    def aquire                     (self, descr,     ttype)    : pass
    @ASYNC
    def aquire                     (self, descr,     ttype)    : pass

    @SYNC
    def release                    (self, id, drain, ttype)    : pass
    @ASYNC
    def release                    (self, id, drain, ttype)    : pass



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

