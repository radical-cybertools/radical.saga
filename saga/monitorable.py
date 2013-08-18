
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2013, The SAGA Project"
__license__   = "MIT"


""" Monitorable interface """

import saga.attributes       as sa
import saga.base             as sb
import saga.exceptions       as se
import saga.utils.signatures as sus


# ------------------------------------------------------------------------------
#
class Monitorable (sa.Attributes) :

    # --------------------------------------------------------------------------
    #
    def __init__ (self) :

        self._attr = super (Monitorable, self)
        self._attr.__init__ ()


    # --------------------------------------------------------------------------
    # 
    # since we have no means to call the saga.Base constructor explicitly (we
    # don't inherit from it), we have to rely that the classes which implement
    # the Monitorable interface are correctly calling the Base constructure --
    # otherwise we won't have an self._adaptor to talk to...
    #
    # This helper method checks the existence of self._adaptor, and should be
    # used before each call forwarding.
    #
    def _check (self) :
        if  not hasattr (self, '_adaptor') :
            raise se.IncorrectState ("object is not fully initialized")


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Monitorable')
    @sus.returns (sus.list_of (basestring))
    def list_metrics (self) :

        self._check ()
        return self._adaptor.list_metrics ()


    # --------------------------------------------------------------------------
    #
    # Metrics are not implemented in SAGA-Python
    #
  # @sus.takes   ('Monitorable', basestring)
  # @sus.returns ('Metric')
  # def get_metric (name) :
  #
  #     self._check ()
  #     return self._adaptor.get_metric (name)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Monitorable',
                  basestring,
                  sus.one_of ('saga.Callback', callable))
    @sus.returns (int)
    def add_callback (self, name, cb) :

        self._check ()
        return self._adaptor.add_callback (name, cb)


    # --------------------------------------------------------------------------
    #
    @sus.takes   ('Monitorable',
                  int)
    @sus.returns (sus.nothing)
    def remove_callback (self, cookie) :

        self._check ()
        return self._adaptor.remove_callback (cookie)



# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

