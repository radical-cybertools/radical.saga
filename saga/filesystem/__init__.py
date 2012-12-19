
# Job attributes:
JOB_ID                = 'JobID'
EXECUTION_HOSTS       = 'ExecutionHosts'
CREATED               = 'Created'
STARTED               = 'Started'
FINISHED              = 'Finished'
EXIT_CODE             = 'ExitCode'
TERMSIG               = 'Termsig'

# Job metrics:
STATE                 = 'State'
STATE_DETAIL          = 'StateDetail'
SIGNAL                = 'Signal'
CPU_TIME              = 'CPUTime'
MEMORY_USE            = 'MemoryUse'
VMEMORY_USE           = 'VmemoryUse'
PERFORMANCE           = 'Performance'

# filesystem flags enum:
OVERWRITE      =    1
RECURSIVE      =    2
DEREFERENCE    =    4
CREATE         =    8
EXCLUSIVE      =   16
LOCK           =   32
CREATE_PARENTS =   64
TRUNCATE       =  128
APPEND         =  256
READ           =  512
WRITE          = 1024
READ_WRITE     = 1536
BINARY         = 2048

# filesystem seek_mode enum:
START          = "Start"
CURRENT        = "Current"
END            = "End"


from saga.filesystem.file       import File
from saga.filesystem.directory  import Directory

