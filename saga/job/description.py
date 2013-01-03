
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
        
        self._attributes_register  (saga.job.EXECUTABLE           , None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.ARGUMENTS            , None, self.STRING, self.VECTOR, self.WRITABLE)
        self._attributes_register  (saga.job.ENVIRONMENT          , None, self.ANY,    self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.SPMD_VARIATION       , None, self.ENUM,   self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.TOTAL_CPU_COUNT      , None, self.INT,    self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.NUMBER_OF_PROCESSES  , None, self.INT,    self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.PROCESSES_PER_HOST   , None, self.INT,    self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.THREADS_PER_PROCESS  , None, self.INT,    self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.WORKING_DIRECTORY    , None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.INTERACTIVE          , None, self.BOOL,   self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.INPUT                , None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.OUTPUT               , None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.ERROR                , None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.FILE_TRANSFER        , None, self.STRING, self.VECTOR, self.WRITABLE)
        self._attributes_register  (saga.job.CLEANUP              , None, self.BOOL,   self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.JOB_START_TIME       , None, self.TIME,   self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.WALL_TIME_LIMIT      , None, self.INT,    self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.TOTAL_PHYSICAL_MEMORY, None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.CPU_ARCHITECTURE     , None, self.ENUM,   self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.OPERATING_SYSTEM_TYPE, None, self.ENUM,   self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.CANDIDATE_HOSTS      , None, self.STRING, self.VECTOR, self.WRITABLE)
        self._attributes_register  (saga.job.QUEUE                , None, self.STRING, self.SCALAR, self.WRITABLE)
        self._attributes_register  (saga.job.JOB_CONTACT          , None, self.STRING, self.VECTOR, self.WRITABLE)

        self._attributes_set_enums (saga.job.SPMD_VARIATION,      ['MPI', 'OpenMP', 'MPICH-G'])

        pass


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

