# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

################################################################################
#
# Job States
UNKNOWN               = 'Unknown'
""" The state of the job could not be determined.
"""

NEW                   = 'New'
""" This state identifies a newly constructed job instance which has not 
    yet been submitted / started to run.
"""

PENDING               = 'Pending'
""" This state identifies a job instance which has been submitted / started 
    but is not yet executing on the resource.
"""

RUNNING               = 'Running'
""" This state identifies a job instance which has been submitted and is 
    currently running.
"""

DONE                  = 'Done'
""" The job has finished succesfully.
    This state is final.
"""

CANCELED              = 'Canceled'
""" The job has been canceled either by the user or by job service.
    This state is final.
"""

FAILED                = 'Failed'
""" The job has finished unsuccessfully / with an error. 
    This state is final.
"""

SUSPENDED             = 'Suspended'
""" The job has been suspended by the job service.
"""

################################################################################
#
# JobDescription attributes:
EXECUTABLE            = 'Executable'
""" :todo: docstring
"""

ARGUMENTS             = 'Arguments'
""" :todo: docstring
"""

SPMD_VARIATION        = 'SPMDVariation'
""" :todo: docstring
"""

TOTAL_CPU_COUNT       = 'TotalCPUCount'
""" :todo: docstring
"""

NUMBER_OF_PROCESSES   = 'NumberOfProcesses'
""" :todo: docstring
"""

PROCESSES_PER_HOST    = 'ProcessesPerHost'
""" :todo: docstring
"""

THREADS_PER_PROCESS   = 'ThreadsPerProcess'
""" :todo: docstring
"""

ENVIRONMENT           = 'Environment'    # dict {string:string} / list [string]
""" :todo: docstring
"""

WORKING_DIRECTORY     = 'WorkingDirectory'
""" :todo: docstring
"""

INTERACTIVE           = 'Interactive'
""" :todo: docstring
"""

INPUT                 = 'Input'
""" :todo: docstring
"""

OUTPUT                = 'Output'
""" :todo: docstring
"""

ERROR                 = 'Error'
""" :todo: docstring
"""

PROJECT               = 'Project'
""" :todo: docstring
"""

FILE_TRANSFER         = 'FileTransfer'
""" :todo: docstring
"""

CLEANUP               = 'Cleanup'
""" :todo: docstring
"""

JOB_START_TIME        = 'JobStartTime'
""" :todo: docstring
"""

TOTAL_CPU_TIME        = 'TotalCPUTime'
""" :todo: docstring
"""

TOTAL_PHYSICAL_MEMORY = 'TotalPhysicalMemory'
""" :todo: docstring
"""

CPU_ARCHITECTURE      = 'CPUArchitecture'
""" :todo: docstring
"""

OPERATING_SYSTEM_TYPE = 'OperatingSystemType'
""" :todo: docstring
"""

CANDIDATE_HOSTS       = 'CandidateHosts'
""" :todo: docstring
"""

QUEUE                 = 'Queue'
""" :todo: docstring
"""

JOB_CONTACT           = 'JobContact'
""" :todo: docstring
"""

################################################################################
# Job attributes:
ID                    = 'ID'
""" :todo: docstring
"""

EXECUTION_HOSTS       = 'ExecutionHosts'
""" :todo: docstring
"""

CREATED               = 'Created'
""" :todo: docstring
"""

STARTED               = 'Started'
""" :todo: docstring
"""

FINISHED              = 'Finished'
""" :todo: docstring
"""

EXIT_CODE             = 'ExitCode'
""" :todo: docstring
"""

TERMSIG               = 'Termsig'
""" :todo: docstring
"""

SERVICE_URL           = 'ServiceUrl'
""" :todo: docstring
"""

################################################################################
# Job metrics:
STATE                 = 'State'
""" Subscribable job state information. This metric gets triggered whenever
    the state of the job changes.
"""

STATE_DETAIL          = 'StateDetail'
""" Allows to get information about the native (backend) job state. For some 
    applications, access to the native backend state model can be important,
    however, it is not guaranteed to be supported by all middleware adaptors.
    Generally, state details is supposed to be formatted as follows::

      ’<model>:<state>’=value

    The STATE_DETAIL metric gets triggered whenever the backend state
    information of the job changes.
"""

SIGNAL                = 'Signal'
""" :todo: docstring
"""

CPU_TIME              = 'CPUTime'
""" :todo: docstring
"""

MEMORY_USE            = 'MemoryUse'
""" :todo: docstring
"""

VMEMORY_USE           = 'VmemoryUse'
""" :todo: docstring
"""

PERFORMANCE           = 'Performance'
""" :todo: docstring
"""

from saga.job.job         import Job
from saga.job.job         import Self
from saga.job.service     import Service
from saga.job.container   import Container
from saga.job.description import Description

