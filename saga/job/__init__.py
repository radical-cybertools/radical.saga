# -*- coding: utf-8 -*-
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

__author__    = "Ole Christian Weidner"
__copyright__ = "Copyright 2012, The SAGA Project"
__license__   = "MIT"

################################################################################
#
# Job States
UNKNOWN               = 'Unknown'
""" :todo: docstring
"""

NEW                   = 'New'
""" :todo: docstring
"""

PENDING               = 'Pending'
""" :todo: docstring
"""

RUNNING               = 'Running'
""" :todo: docstring
"""

DONE                  = 'Done'
""" :todo: docstring
"""

CANCELED              = 'Canceled'
""" :todo: docstring
"""

FAILED                = 'Failed'
""" :todo: docstring
"""

SUSPENDED             = 'Suspended'
""" :todo: docstring
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
JOB_ID                = 'JobID'
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

################################################################################
# Job metrics:
STATE                 = 'State'
""" :todo: docstring
"""

STATE_DETAIL          = 'StateDetail'
""" :todo: docstring
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
from saga.job.description import Description

