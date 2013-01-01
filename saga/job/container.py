# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

from saga.engine.logger import getLogger

import saga.task

# 'forward' declaration of job.Container
class Container(saga.task.Container):
    """ :todo: document me

        .. py:attribute:: jobs

           The (read-only) `jobs` property returns a list of all job objects in the container.

           :rtype: :class:`saga.job.Job` list

        .. py:attribute:: size

           The (read-only) `size` property returns the number of job objectis in the container.

           :rtype: int

        .. py:attribute:: states

           The (read-only) `states` property returns a list of states that represent the 
           states of the individual jobs in the container. 

           :rtype: list
    """
    def __init__ (self) :

        saga.task.Container.__init__(self)

        self._attributes_register   ("Jobs",   [],    self.ANY, self.VECTOR, self.READONLY)
        self._attributes_set_getter ("Jobs",   self.get_tasks)

        self._logger = getLogger ('saga.job.Container')



