
from saga.Object     import Object
from saga.Attributes import Attributes

class Description(Object, Attributes)

    def __init__(self):

        # set attribute interface properties
        self.attributes_extensible_  (False)
        self.attributes_camelcasing_ (True)

        # register properties with the attribute interface
        self.attributes_register_  (EXECUTABLE           , None, self.String, self.Scalar, self.Writable)
        self.attributes_register_  (ARGUMENTS            , None, self.String, self.Vector, self.Writable)
        self.attributes_register_  (ENVIRONMENT          , None, self.Any,    self.Scalar, self.Writable)
        self.attributes_register_  (SPMD_VARIATION       , None, self.Enum,   self.Scalar, self.Writable)
        self.attributes_register_  (TOTAL_CPU_COUNT      , None, self.Int,    self.Scalar, self.Writable)
        self.attributes_register_  (NUMBER_OF_PROCESSES  , None, self.Int,    self.Scalar, self.Writable)
        self.attributes_register_  (PROCESSES_PER_HOST   , None, self.Int,    self.Scalar, self.Writable)
        self.attributes_register_  (THREADS_PER_PROCESS  , None, self.Int,    self.Scalar, self.Writable)
        self.attributes_register_  (WORKING_DIRECTORY    , None, self.String  self.Scalar, self.Writable)
        self.attributes_register_  (INTERACTIVE          , None, self.Bool,   self.Scalar, self.Writable)
        self.attributes_register_  (INPUT                , None, self.String, self.Scalar, self.Writable)
        self.attributes_register_  (OUTPUT               , None, self.String, self.Scalar, self.Writable)
        self.attributes_register_  (ERROR                , None, self.String, self.Scalar, self.Writable)
        self.attributes_register_  (FILE_TRANSFER        , None, self.String, self.Vector, self.Writable)
        self.attributes_register_  (CLEANUP              , None, self.Bool,   self.Scalar, self.Writable)
        self.attributes_register_  (JOB_START_TIME       , None, self.Time,   self.Scalar, self.Writable)
        self.attributes_register_  (TOTAL_CPU_TIME       , None, self.Time,   self.Scalar, self.Writable)
        self.attributes_register_  (TOTAL_PHYSICAL_MEMORY, None, self.String, self.Scalar, self.Writable)
        self.attributes_register_  (CPU_ARCHITECTURE     , None, self.Enum,   self.Scalar, self.Writable)
        self.attributes_register_  (OPERATING_SYSTEM_TYPE, None, self.Enum,   self.Scalar, self.Writable)
        self.attributes_register_  (CANDIDATE_HOSTS      , None, self.String, self.Vector, self.Writable)
        self.attributes_register_  (QUEUE                , None, self.String, self.Scalar, self.Writable)
        self.attributes_register_  (JOB_CONTACT          , None, self.String, self.Vector, self.Writable)

        self.attributes_set_enums_ (SPMD_VARIATION,      ['MPI', 'OpenMP', 'MPICH-G'])

        pass

