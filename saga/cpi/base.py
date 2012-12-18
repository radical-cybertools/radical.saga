# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" Provides the SAGA runtime. """

from saga.engine.config import Configurable

class Base (Configurable) :

    def __init__ (self, adaptor_name, config_options={}) :
        self._adaptor_name = adaptor_name

        Configurable.__init__ (self, adaptor_name, config_options)

    def _get_name (self) :
        return self._adaptor_name

