
UNKNOWN               = 'Unknown'
NEW                   = 'New'
RUNNING               = 'Running'
DONE                  = 'Done'
CANCELED              = 'Canceled'
FAILED                = 'Failed'
SUSPENDED             = 'Suspended'

# JobDescription attributes:
EXECUTABLE            = 'Executable'
ARGUMENTS             = 'Arguments'
SPMD_VARIATION        = 'SPMDVariation'
TOTAL_CPU_COUNT       = 'TotalCPUCount'
NUMBER_OF_PROCESSES   = 'NumberOfProcesses'
PROCESSES_PER_HOST    = 'ProcessesPerHost'
THREADS_PER_PROCESS   = 'ThreadsPerProcess'
ENVIRONMENT           = 'Environment'    # dict {string:string} / list [string]
WORKING_DIRECTORY     = 'WorkingDirectory'
INTERACTIVE           = 'Interactive'
INPUT                 = 'Input'
OUTPUT                = 'Output'
ERROR                 = 'Error'
FILE_TRANSFER         = 'FileTransfer'
CLEANUP               = 'Cleanup'
JOB_START_TIME        = 'JobStartTime'
TOTAL_CPU_TIME        = 'TotalCPUTime'
TOTAL_PHYSICAL_MEMORY = 'TotalPhysicalMemory'
CPU_ARCHITECTURE      = 'CPUArchitecture'
OPERATING_SYSTEM_TYPE = 'OperatingSystemType'
CANDIDATE_HOSTS       = 'CandidateHosts'
QUEUE                 = 'Queue'
JOB_CONTACT           = 'JobContact'

# Job attributes:
JOB_ID                = 'JobID'
EXECUTION_HOSTS       = 'ExecutionHosts'
CREATED               = 'Created'
STARTED               = 'Started'
FINISHED              = 'Finished'
EXIT_CODE             = 'ExitCode'
TERMSIG               = 'Termsig'
# WORKING_DIRECTORY   = 'WorkingDirectory'  # collision

# Job metrics:
STATE                 = 'State'
STATE_DETAIL          = 'StateDetail'
SIGNAL                = 'Signal'
CPU_TIME              = 'CPUTime'
MEMORY_USE            = 'MemoryUse'
VMEMORY_USE           = 'VmemoryUse'
PERFORMANCE           = 'Performance'

