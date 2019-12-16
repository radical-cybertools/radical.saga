
__author__    = "Andre Merzky, Ole Weidner, Thomas Schatz"
__copyright__ = "Copyright 2012-2013, The SAGA Project"
__license__   = "MIT"


""" SAGA job description interface """

import radical.utils.signatures   as rus

from ..         import attributes as sa
from .constants import *


# TODO: file transfer to accept strings *and* dicts like in RP
# TODO: flags: cacheable: don't stage twice in same session
#              remove   : remove src after staging


# ------------------------------------------------------------------------------
#
class Description (sa.Attributes) :
    """ The job description class. """

    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Description')
    @rus.returns (rus.nothing)
    def __init__(self):

        # set attribute interface properties

        self._attributes_extensible  (True)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface

        self._attributes_register  (EXECUTABLE           , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (PRE_EXEC             , None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (POST_EXEC            , None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (ARGUMENTS            , None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (ENVIRONMENT          , None, sa.STRING, sa.DICT,   sa.WRITEABLE)
        self._attributes_register  (TOTAL_CPU_COUNT      , None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (TOTAL_GPU_COUNT      , None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (NUMBER_OF_PROCESSES  , None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (PROCESSES_PER_HOST   , None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (THREADS_PER_PROCESS  , None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (WORKING_DIRECTORY    , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (INTERACTIVE          , None, sa.BOOL,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (INPUT                , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (OUTPUT               , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (ERROR                , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (FILE_TRANSFER        , None, sa.ANY,    sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (CLEANUP              , None, sa.BOOL,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (JOB_START_TIME       , None, sa.TIME,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (WALL_TIME_LIMIT      , None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (TOTAL_PHYSICAL_MEMORY, None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (CPU_ARCHITECTURE     , None, sa.ENUM,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (OPERATING_SYSTEM_TYPE, None, sa.ENUM,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (CANDIDATE_HOSTS      , None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (QUEUE                , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (NAME                 , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (PROJECT              , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (JOB_CONTACT          , None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (SPMD_VARIATION       , None, sa.ENUM,   sa.SCALAR, sa.WRITEABLE)

        self._attributes_set_enums (SPMD_VARIATION,      ['MPI', 'OpenMP', 'MPICH-G'])

        self._env_is_list = False

        self._attributes_set_getter (ENVIRONMENT, self._get_env)
        self._attributes_set_setter (ENVIRONMENT, self._set_env)


    # --------------------------------------------------------------------------
    #
    def _set_env (self, val) :
        if  isinstance (val, list) :
            self._env_is_list = True


    # --------------------------------------------------------------------------
    #
    def _get_env (self) :
        env = self.get_attribute (ENVIRONMENT)
        if  self._env_is_list :
            self._env_is_list = False
            return ["%s=%s" % (key, val) for (key, val) in list(env.items ())]
        return env


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Description',
                  ('Description', dict))
    @rus.returns ('Description')
    def __deepcopy__ (self, memo) :
        other = Description ()
        return self.clone (other)


    # --------------------------------------------------------------------------
    #
    @rus.takes   ('Description',
                  'Description')
    @rus.returns ('Description')
    def clone (self, other=None) :
        """
        clone()

        Implements deep copy. u

        Unlike the default python assignment (copy object reference),
        a deep copy will create a new object instance with the same state --
        after a deep copy, a change on one instance will not affect the other.
        """

        # a job description only has attributes - so create a new instance,
        # clone the attribs, and done.
        if not other :
            other = Description ()

        return self._attributes_deep_copy (other)


# ------------------------------------------------------------------------------

