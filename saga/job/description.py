
__author__    = "Andre Merzky"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

""" SAGA job description interface """

import saga

class Description (saga.Attributes) :
    """ The job description class. """

    def __init__(self):

        # set attribute interface properties

        import saga.attributes as sa

        self._attributes_extensible  (False)
        self._attributes_camelcasing (True)

        # register properties with the attribute interface
        
        self._attributes_register  (saga.job.EXECUTABLE           , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.ARGUMENTS            , None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (saga.job.ENVIRONMENT          , None, sa.ANY,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.SPMD_VARIATION       , None, sa.ENUM,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.TOTAL_CPU_COUNT      , None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.NUMBER_OF_PROCESSES  , None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.PROCESSES_PER_HOST   , None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.THREADS_PER_PROCESS  , None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.WORKING_DIRECTORY    , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.INTERACTIVE          , None, sa.BOOL,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.INPUT                , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.OUTPUT               , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.ERROR                , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.FILE_TRANSFER        , None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (saga.job.CLEANUP              , None, sa.BOOL,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.JOB_START_TIME       , None, sa.TIME,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.WALL_TIME_LIMIT      , None, sa.INT,    sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.TOTAL_PHYSICAL_MEMORY, None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.CPU_ARCHITECTURE     , None, sa.ENUM,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.OPERATING_SYSTEM_TYPE, None, sa.ENUM,   sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.CANDIDATE_HOSTS      , None, sa.STRING, sa.VECTOR, sa.WRITEABLE)
        self._attributes_register  (saga.job.QUEUE                , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.PROJECT              , None, sa.STRING, sa.SCALAR, sa.WRITEABLE)
        self._attributes_register  (saga.job.JOB_CONTACT          , None, sa.STRING, sa.VECTOR, sa.WRITEABLE)

        self._attributes_set_enums (saga.job.SPMD_VARIATION,      ['MPI', 'OpenMP', 'MPICH-G'])

        pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

