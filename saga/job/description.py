
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" SAGA job description interface
"""

import saga


class Description (saga.Attributes) :
    """ The job description class.
    """

    def __init__(self):

        # set attribute interface properties
        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        
        self._attributes_register  (saga.job.EXECUTABLE           , None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.ARGUMENTS            , None, STRING, VECTOR, WRITEABLE)
        self._attributes_register  (saga.job.ENVIRONMENT          , None, ANY,    SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.SPMD_VARIATION       , None, ENUM,   SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.TOTAL_CPU_COUNT      , None, INT,    SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.NUMBER_OF_PROCESSES  , None, INT,    SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.PROCESSES_PER_HOST   , None, INT,    SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.THREADS_PER_PROCESS  , None, INT,    SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.WORKING_DIRECTORY    , None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.INTERACTIVE          , None, BOOL,   SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.INPUT                , None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.OUTPUT               , None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.ERROR                , None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.FILE_TRANSFER        , None, STRING, VECTOR, WRITEABLE)
        self._attributes_register  (saga.job.CLEANUP              , None, BOOL,   SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.JOB_START_TIME       , None, TIME,   SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.WALL_TIME_LIMIT      , None, INT,    SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.TOTAL_PHYSICAL_MEMORY, None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.CPU_ARCHITECTURE     , None, ENUM,   SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.OPERATING_SYSTEM_TYPE, None, ENUM,   SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.CANDIDATE_HOSTS      , None, STRING, VECTOR, WRITEABLE)
        self._attributes_register  (saga.job.QUEUE                , None, STRING, SCALAR, WRITEABLE)
        self._attributes_register  (saga.job.JOB_CONTACT          , None, STRING, VECTOR, WRITEABLE)

        self._attributes_set_enums (saga.job.SPMD_VARIATION,      ['MPI', 'OpenMP', 'MPICH-G'])

        pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

