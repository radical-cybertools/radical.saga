
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


import radical.utils.signatures as rus

import saga.adaptors.base       as sab
import saga.attributes          as sa
import saga.session             as ss
import saga.task                as st
import saga.url                 as surl

from   saga.messages.constants  import *
from   saga.constants           import SYNC, ASYNC, TASK


# ------------------------------------------------------------------------------
#
class Message (sa.Attributes) :


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Message', 
                  rus.optional (int, rus.nothing)) # size
    @rus.returns (rus.nothing)
    def __init__ (self, size=None) : 
        '''
        size:      expected size of buffer when used -- informative, not normative
        ret:       Message
        '''

        # set attribute interface properties
        self._attributes_camelcasing   (True)
        self._attributes_allow_private (True)
        self._attributes_extensible    (True, getter=self._attribute_getter, 
                                              setter=self._attribute_setter,
                                              lister=self._attribute_lister,
                                              caller=self._attribute_caller)

        # register properties with the attribute interface 
        self._attributes_register   (ID,     None, sa.STRING, sa.SCALAR, sa.READONLY)
        self._attributes_register   (SENDER, None, sa.STRING, sa.SCALAR, sa.READONLY)


    # --------------------------------------------------------------------------
    #
    # class methods
    #
    @rus.takes   ('Message')
    @rus.returns (rus.anything)
    def get_id (self) :

        return self._adaptor.get_id ()


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Message')
    @rus.returns (rus.anything)
    def get_sender (self) :

        return self._adaptor.get_sender ()


    # --------------------------------------------------------------------------
    #
    # attribute methods
    #
    @rus.takes   ('Message', 
                  basestring)
    @rus.returns (rus.anything)
    def _attribute_getter (self, key) :

        return self._adaptor.attribute_getter (key)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Message', 
                  basestring,
                  rus.anything)
    @rus.returns (rus.nothing)
    def _attribute_setter (self, key, val) :

        return self._adaptor.attribute_setter (key, val)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Message')
    @rus.returns (rus.list_of (rus.anything))
    def _attribute_lister (self) :

        return self._adaptor.attribute_lister ()


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Message', 
                  basestring, 
                  int, 
                  callable)
    @rus.returns (rus.anything)
    def _attribute_caller (self, key, id, cb) :

        return self._adaptor.attribute_caller (key, id, cb)


# ------------------------------------------------------------------------------

